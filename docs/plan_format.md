# 计划格式说明（output/plans/<query_id>.json）

每个查询一份计划文档，只基于 Parquet footer 统计生成，计划阶段不读取任何列数据。

## 顶层结构

```json
{
  "query_id": "range",
  "predicate": { ... 原始谓词树 ... },
  "rule": "剪枝总规则（人读）",
  "files": [ { "file": "<绝对路径>", "row_groups": [ ... ] } ]
}
```

`files` 顺序与 `fixtures/files.json` 一致。

## 行组条目

| 字段 | 含义 |
| --- | --- |
| `row_group` | 行组在文件内的物理索引 |
| `num_rows` | footer 记录的行数 |
| `action` | `keep`（保留，执行阶段扫描）或 `skip`（证明无 TRUE 行，跳过） |
| `reason` | 人读决策理由（跳过依据 / 保守保留依据） |
| `can_be_true` | 组内是否可能存在谓词为 TRUE 的行 |
| `can_be_false` | 组内是否可能存在谓词为 FALSE 的行（UNKNOWN 行不计入） |
| `missing_or_unreliable_stats` | 谓词涉及但统计缺失/不可靠的列 |
| `trace` | 递归推理树，结构与谓词树一一对应 |

## trace 节点

```json
{
  "op": "and | or | not | eq | ne | lt | le | gt | ge | is_null",
  "detail": "人读表达式片段",
  "can_be_true": true,
  "can_be_false": true,
  "stats_used": { "<列>": { "min": ..., "max": ..., "null_count": ...,
                           "min_max_reliable": ..., "null_count_reliable": ...,
                           "unknown_reasons": [...] } },
  "unknown": ["统计不可靠时的保守退化说明"],
  "children": [ 子节点 ]
}
```

## 推理语义（SQL 三值逻辑）

对每个子树求两个独立结论，UNKNOWN 既不是 TRUE 也不是 FALSE：

- 比较叶子：可靠 min/max 下按值域判定；`has_min_max=False`（常见于全 NULL 组）或
  物理类型不受支持时退化为 `(can_be_true=true, can_be_false=true)` 保守保留；
- `IS NULL`：仅在 `null_count` 可靠时推理，`null_count=0` 可证明跳过，否则保守；
- `NOT`：两个结论**互换**（`can_be_true'=can_be_false`，反之亦然），
  绝不把 UNKNOWN 当 FALSE 后取反；
- `AND`：任一子树不可能 TRUE 则整体不可能 TRUE；任一子树可能 FALSE 则整体可能 FALSE；
- `OR`：任一子树可能 TRUE 则整体可能 TRUE；所有子树都可能 FALSE 时整体才可能 FALSE。

AND/OR 只组合**独立子树**的结论，不假设不同列的极值来自同一行。
只有 `can_be_true=false` 被证明时才输出 `skip`。

# 运行记录格式（output/run_record.json）

```json
{
  "note": "声明：记录的是工具发起的列读取请求，非 OS 磁盘字节……",
  "files": ["stats.parquet", "no-stats.parquet"],
  "queries": [
    {
      "query_id": "range",
      "output_path": "output/results/range.parquet",
      "result_rows": 4,
      "groups": [
        {
          "file": "...", "row_group": 3, "num_rows": 4,
          "status": "planned_skip | no_hits_after_filter | matched",
          "matched_rows": 2,
          "filter_columns": ["k"],
          "projection_only_columns": ["row_id", "payload"],
          "column_reads": [
            { "file": "...", "row_group": 3, "phase": "filter", "columns": ["k"] },
            { "file": "...", "row_group": 3, "phase": "projection", "columns": ["row_id", "payload"] }
          ],
          "note": "..."
        }
      ],
      "read_requests": [ 与 groups 内登记一致的扁平请求列表 ]
    }
  ]
}
```

三类行组状态的区别：

- `planned_skip`：计划阶段已证明无 TRUE 行，执行阶段**零**列读取；
- `no_hits_after_filter`：计划保留，执行时只读过滤列，精确过滤后无命中，
  **不读任何投影专用列**；
- `matched`：有命中行；先读过滤列，再读剩余投影列。过滤列若同时在投影中，
  直接复用第一阶段的数组，`column_reads` 中不会出现该列的第二次请求。

请求由 `InstrumentedParquetFile.read_row_group` 在真实调用发生时登记，
计划器不参与填写；空列集合不会产生请求。记录中不含字节数，
footer 的 `total_compressed_size` 仅存在于 footer，不冒充实际 I/O。

# 结果 Parquet（output/results/<query_id>.parquet）

- 投影列按 `queries.json` 中 `columns` 的顺序排列，后附三列溯源：
  `_source_file`、`_source_row_group`、`_source_row_in_group`；
- 行序：文件顺序（files.json）→ 行组物理索引 → 组内物理行号；
- 跨文件的重复数据原样保留；空结果写出同 schema 的零行 Parquet。

# 标记查询（嵌套探头读数）

queries.json 中含 `"exists"` 键的对象按标记查询处理，与平面查询可混排：

```json
{
  "id": "m_a",
  "marker": "A",
  "exists": [{"eq": ["probe", "P"]}, {"ge": ["value", 8]}],
  "negate": false,
  "record": {"device": "device", "ts": "ts", "sample_id": "sample_id", "readings": "readings"}
}
```

- `exists`：非空条件列表，整组条件在同一个读数元素上合取；条件只支持
  `eq/ne/lt/le/gt/ge`（`[字段名, 字面量]`）与 `is_null`（字段名），不嵌套逻辑运算；
- `negate`：可选，默认 `false`；为 `true` 时对整个存在条件取反，仍只输出真；
- `record`：可选的列名映射，默认如上；四个键缺一不可改、值不可重复。

## 标记事实 Parquet（output/results/<id>.parquet）

固定 schema（批处理按此契约读取）：

| 列 | 类型 | 含义 |
| --- | --- | --- |
| `marker` | string | 标记名 |
| `device` / `ts` / `sample_id` | 与源记录相同 | 命中采样的设备、整数时刻、唯一采样号 |
| `hit_indices` | `list<int64>` | 全部真元素的原始下标；取反而命中时为空列表 |
| `_source_file` / `_source_row_group` / `_source_row_in_group` | string / int32 / int64 | 物理出处 |

行序与平面查询一致；空结果写出同 schema 的零行 Parquet。

## 标记查询计划（output/plans/<id>.json）

结构与平面计划相同（`files[].row_groups[]` 的 action/reason/trace），
`trace` 根节点 `op` 为 `exists` 或 `not`，子节点是各条件的单列证伪轨迹：

- 每个条件只用**自身读数叶列**（如 `readings.list.element.probe`）的 footer
  min/max（`is_null` 用 `null_count=0`）判断“是否可能有元素满足”，只证伪不证实；
- 任一条件被证伪 -> 整组条件不可能为真 -> `skip`；统计缺失/不可靠 ->
  该条件保守为真并在 `missing_or_unreliable_stats` 登记；
- 取反查询无法仅凭 footer 证明“不存在确定为假的行”，全部 `keep`，
  trace 的 `unknown` 中登记该原因。

# 台架程序批处理结算（output/settlement/）

批描述 JSON（`--batch`）：

```json
{
  "id": "batch-ab",
  "contract": {"steps": ["A", "B"], "max_gaps": [3]},
  "marker_files": ["../../output/results/m_a.parquet", "../../output/results/m_b.parquet"]
}
```

- `steps`：1..4 个按顺序的标记名；`max_gaps`：相邻两步允许的最大时间差
  （长度 = `steps` 数 - 1，非负整数，边界值可用）；
- `marker_files` 相对批描述文件解析；`--markers` 可整体覆盖；每批固定一个合同。

## settlement/<batch_id>.parquet

每行是某选中程序的一步：

| 列 | 含义 |
| --- | --- |
| `device` / `program_index` / `step_index` | 设备、程序序号（按首时刻升序）、步序号 |
| `marker` / `ts` / `sample_id` / `hit_indices` | 该步的标记事实原文 |
| `program_span` / `program_start_ts` / `program_end_ts` | 程序闭区间与跨度 |
| `_source_file` / `_source_row_group` / `_source_row_in_group` | 该步事实的物理出处 |

## settlement/<batch_id>.json

人读报告：`contract`、`limits`（最多 4 步 / 每设备最多 60 采样）、
`devices[]`（每台设备的标记事实数、候选程序数、选中程序数、总跨度，
以及每段程序的区间与各步 `marker/ts/sample_id/hit_indices/source`）。

挑选规则：每台设备上段数最多；并列先取总跨度较小；再按各段依次使用的
采样号序列字典序定胜负。闭区间互不重叠（前段末时刻 < 后段首时刻）。
