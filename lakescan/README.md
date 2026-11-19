# lakescan — 离线湖表快照读取库

针对一份离线湖表备份（Parquet 文件 + `table.json` 合同）实现**按指定快照扫描**的读取库，
交付带来源位置 `(file_id, row_position)` 的正确关系与查询解释。

明确**只做**离线表合同规定的事：不实现完整 Iceberg 协议，不写湖，不做通用 SQL 引擎，
不做增量订阅或提交协议。PyArrow 负责 Parquet 解码与列计算；本库实现跨文件的可见行、
字段身份与读取规划。

## 合同（table.json）

- `schemas`：不可变 schema 列表。顶层原始类型字段以**正整数 `field_id`** 标识
  （`int32` / `int64` / `decimal(p,s)` / `utf8` / `boolean`）。重命名不改变身份；
  允许的演化只有精确扩宽：`int32→int64`、同 scale 的 decimal 精度扩宽、utf8、布尔。
  同一列名后来对应新 ID 是另一个字段，不继承旧值。
- `snapshots`：每个快照声明**完整** `active_files` 集合与 `parent`。快照可分叉，
  父链仅用于解释；读取只以该快照的 `active_files` 为准。
- 文件条目：`file_id`、`kind`（`DATA`/`POS_DELETE`/`EQ_DELETE`）、`path`、`sha256`、
  `partition`（明确的等值元组；空值与缺字段不同）、`data_seq`、`file_seq`
  （**仅身份**，不参与删除适用性）、`schema_id`；`EQ_DELETE` 另有 `eq_field_ids`。
- 文件按摘要定位：复制目录后照常读取；源字节被改动 → `SourceVersionMismatch`，
  不继续使用旧偏移结果。

## 删除语义

- **POS_DELETE** `(target_file_id, row_position)`：仅当目标是本快照 active DATA、
  `target.data_seq <= delete.data_seq`（非严格）、分区相同且位置合法时适用。
  找不到的目标保留为 `inactive_target` 诊断（按 `file_id` 匹配，同内容其他文件不受
  影响）；越界位置属于快照错误。位置是文件物理行号，从 0 起、跨 row group 连续。
- **EQ_DELETE** 一组字段 ID + 键行：仅作用于分区相同且 `DATA.data_seq` **严格小于**
  `delete.data_seq` 的数据文件。null 键等于 null；键是精确类型化值（不拼接字符串、
  不做 float 近似、不凭列名匹配）；缺失的可空字段按 null 参与；比较只使用允许的
  精确扩宽。非法收窄、未知 ID、重复 ID、不可表示键在**扫描前**拒绝。
- 重复删除与两种删除重叠只删该物理行一次；相同业务 `row_uid` 在不同物理位置是两行，
  不按业务号去重。
- 删除键集合可远超内存：键存放于 SQLite 落盘索引（有界页缓存 + 分块写入/探测），
  不进入 Python set。

## 读取

入口：`snapshot_id`、目标 `schema_id`、投影字段 ID、AND/OR/NOT 组合的
`eq`/`lt`/`is_null` 谓词（SQL 三值逻辑，仅 TRUE 入选）。删除匹配先于最终投影；
允许统计信息剪枝但仅作保守排除（缺失/不可信统计保留候选），输出与不剪枝全扫一致。
输出为流式列批 + `(file_id, row_position)`。

## 命令

```bash
# 构建演练夹具（parquet + table.json）
python3 -m lakescan build-fixtures --out /tmp/drill

# 扫描一个固定快照（JSONL，含来源位置）
python3 -m lakescan scan --table /tmp/drill --snapshot s_main --schema 2 --project 1,2
python3 -m lakescan scan --table /tmp/drill --snapshot s_3vl --schema 2 --project 1 \
    --predicate pred.json          # {"op":"lt","field_id":2,"literal":{"kind":"int","value":3}}

# 查询解释：读取计划（文件、剪枝、删除适用性、诊断、父链）
python3 -m lakescan explain-plan --table /tmp/drill --snapshot s_main --schema 2 --project 1,2
# 单行解释：该行因哪个删除文件被排除
python3 -m lakescan explain-row --table /tmp/drill --snapshot s_main --file-id A --row 0

# 独立对照：不调用库的删除应用函数，手工物理行表 + 十进制整数计算
python3 verify.py --table /tmp/drill        # 或 python3 -m lakescan verify --table /tmp/drill

# 验收测试
python3 -m pytest tests/ -q
```

## 跨快照业务键差异导出（compare）

在两个**固定**快照之间做一次性、可重复的业务记录差异。这不是订阅、不写湖、
也不是通用 Iceberg 协议：调用方给出源快照、目标快照、目标 schema 和一个或
多个稳定业务键的 `field_id`，工具分别按**各快照自己的** `active_files` 与
POS/EQ 删除语义扫描，再以业务键配对，输出 `insert` / `delete` / `update`
三类记录及 before/after 投影。

### 比较合同

- 两个快照可位于不同分支；`parent` 仅用于诊断，绝不代替实际快照成员——读取
  只解析每个快照自己的 `active_files`，不会从别的分支拉入文件。
- 业务键字段按 `field_id` 在两侧匹配，必须在两侧都存在，且能按精确扩宽规则
  兼容到目标 schema 的键类型。可空非键字段在数据文件 schema 中缺失时按 null
  补齐。键值**不字符串化、不做浮点近似**：int32/int64 在同一带符号整数域编码，
  decimal 按精确 scale 的整数编码，utf8/boolean 各有类型标签。
- 每个快照的**可见行**中，业务键必须非 null 且唯一。出现 null 键或重复键时
  整个比较失败（`NullKeyError` / `DuplicateKeyError`），绝不任意配对。
- `row_uid` 与物理文件位置**不是**跨快照业务身份。压实、更换文件、同一
  `row_uid` 的重复物理行都不改变业务键规则：只有键集合差与目标 schema 下的
  规范化非键字段值决定变更。仅文件布局 / row group / 源文件位置变化不产生
  业务变更。
- 字段重命名但 `field_id` 不变仍是同一字段；同名但新 `field_id` 是新字段，
  不继承旧值。
- 以键集合差判定新增/删除；同键的规范化非键字段值不同才是 `update`；未变化
  行不输出。输出按规范化键稳定排序，每条记录含变更类型、键、两侧快照 ID、
  完整 before/after 投影（insert 无 before，delete 无 after）。投影覆盖整个
  目标 schema，用户无法通过投影省略比较所需的键或值。

### 有界内存与原子发布

- 数据可能远超内存。每侧流式写入各自的 SQLite 落盘表（受限页缓存、分块写入、
  主键索引保证唯一性），**不把全量键放入 Python dict/set**；再对两份有序
  落盘表做外部归并，归并过程除当前行外只有 O(1) Python 状态。
- 结果先写到目标同目录的临时产物并 `fsync`；只有两侧输入完整校验、两侧键
  唯一性通过且导出成功后才 `os.replace` 原子发布。失败时删除临时文件，绝不
  留下看似完整的目标，也不会用半成品覆盖已存在目标。
- 相同输入重复运行得到**字节稳定**的记录顺序与内容：所有次序都来自规范化
  键字节，与输入文件列举顺序、row group 划分无关。

### 命令与库接口

```bash
# 构建可重建的 diff 演练表（运行时生成，仓库不携带数据产物）
python3 -m lakescan build-diff-fixtures --out /tmp/diffdrill

# 两个快照的键差异（JSONL，原子发布到 --out）
python3 -m lakescan compare \
    --table /tmp/diffdrill --source d_source --target d_target \
    --schema 20 --key-fields 2 --out /tmp/diffdrill/diff.jsonl
# 复合业务键
python3 -m lakescan compare --table DIR --source s0 --target s1 \
    --schema 1 --key-fields 2,7 --out diff.jsonl --work-dir /tmp/diff-work
```

```python
from lakescan import TableDiffer

result = TableDiffer(table_root).compare(
    source_snapshot="d_source", target_snapshot="d_target",
    target_schema_id=20, key_field_ids=(2,), output_path="diff.jsonl")
print(result.stats.to_json())   # insert/delete/update/unchanged + 两侧可见行数
```

### 独立对照

`verify_compare.py` **不导入任何 lakescan 代码**：它自己重读 `table.json`、
用朴素 Python 物理行表应用 POS/EQ 删除、自己做精确规范化与键集合差，再以
子进程方式调用 `compare` CLI 并逐记录、逐字节核对（含重跑字节稳定性、null/
重复键拒绝且无产物）。

```bash
python3 verify_compare.py --table /tmp/diffdrill
```

## 布局

- `lakescan/schema.py` — 字段身份、精确扩宽规则、键的规范编码
- `lakescan/table.py` — table.json 加载/校验、快照图、摘要核验
- `lakescan/deletes.py` — POS/EQ 删除加载与 SQLite 落盘索引、诊断
- `lakescan/predicates.py` — 谓词模型、三值逻辑求值、保守剪枝
- `lakescan/scan.py` — 读取规划与流式执行、explain
- `lakescan/fixtures.py` — 单快照演练夹具构建
- `lakescan/diff.py` — 跨快照键差异：两侧落盘存储、外部归并、原子发布
- `lakescan/diff_fixtures.py` — diff 演练表的运行时生成（含逐键手工推导）
- `verify.py` — 单快照独立对照（与库零共享代码）
- `verify_compare.py` — 跨快照 diff 独立对照（不导入库，朴素物理行表 + 子进程 CLI）
- `tests/test_acceptance.py` — 单快照验收套件（含运行时生成的大删除表落盘/内存上限验证）
- `tests/test_diff.py` — 跨快照 diff 验收套件（分支、三类变更、删除作用域、
  schema 演化、null/重复键拒绝、损坏不发布、原子替换、有界内存、顺序/row group 不变性）
