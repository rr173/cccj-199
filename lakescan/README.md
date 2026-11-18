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

## 布局

- `lakescan/schema.py` — 字段身份、精确扩宽规则、键的规范编码
- `lakescan/table.py` — table.json 加载/校验、快照图、摘要核验
- `lakescan/deletes.py` — POS/EQ 删除加载与 SQLite 落盘索引、诊断
- `lakescan/predicates.py` — 谓词模型、三值逻辑求值、保守剪枝
- `lakescan/scan.py` — 读取规划与流式执行、explain
- `lakescan/fixtures.py` — 演练夹具构建
- `verify.py` — 独立对照（与库零共享代码）
- `tests/test_acceptance.py` — 验收套件（含运行时生成的大删除表落盘/内存上限验证）
