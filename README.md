# Parquet footer 统计驱动的保守 plan/scan

用 Python 3.11 + PyArrow 19 实现的小型 Parquet 查询工具：计划阶段只读 footer 统计、
保守剪枝行组；执行阶段先读过滤列做 SQL 三值逻辑精确过滤，再只为命中行读投影列。
不解析 SQL、不做损坏恢复、不使用 page index、不含远程 Range 服务与性能压测。

除原有的平面查询外，本工具还支持**台架记录上的嵌套探头读数标记查询**与
**台架程序批处理结算**（见下文「嵌套探头读数标记」与「台架程序批处理结算」）。

## 目录

```
fixtures/                 输入：files.json、queries.json、stats.parquet、no-stats.parquet
fixtures/bench/           台架样例：bench-a.parquet、bench-b.parquet、files.json、
                          queries.json（标记查询）、batch-*.json（三份批合同）
tools/make_fixtures.py    从同一份数据生成“有统计/无统计”两个 Parquet（行组与行序一致）
tools/make_bench_fixtures.py  生成台架记录夹具与批合同
src/pqscan/
  expr.py       谓词树解析、schema 校验、PyArrow 表达式翻译、纯 Python 3VL 逐行求值
  stats.py      只读 footer 的行组统计提取与可靠性判定
  planner.py    三值逻辑保守行组推理（can_be_true / can_be_false、trace）
  scanner.py    两阶段列读取、精确行选择、读取请求在执行入口即时登记
  reference.py  独立核对：无剪枝全扫描 + 纯 Python 3VL（与引擎不同代码路径）
  nested.py     嵌套探头读数：标记查询解析/校验、3VL 语义、Arrow 引擎、
                读数叶列 footer 计划、两阶段扫描、独立全扫描核对
  settle.py     台架程序批处理结算：合同校验、候选枚举、三档规则最优挑选
  runner.py     串联计划/扫描/核对，落盘 plans、results、run_record、verification
  cli.py        命令行入口（run / verify / settle）
tests/          pytest 套件（35 个用例）
docs/plan_format.md  计划、运行记录、标记事实与结算结果的格式契约
output/         运行产物（git 忽略）
```

## 运行

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
.venv/bin/python tools/make_fixtures.py           # 重新生成两个夹具（可选，仓库已带）
PYTHONPATH=src .venv/bin/python -m pqscan.cli run \
    --files fixtures/files.json --queries fixtures/queries.json --out output
PYTHONPATH=src .venv/bin/python -m pytest tests/ -q
```

输出：`output/plans/*.json`（行组选择说明）、`output/results/*.parquet`（查询结果）、
`output/run_record.json`（实际列读取请求）、`output/verification.json`（独立核对结论）。

## 台架样例（可复跑）

```bash
.venv/bin/python tools/make_bench_fixtures.py      # 生成 fixtures/bench/（仓库已带）
# 1) 标记查询：产出标记事实 output/results/m_{a,b,c,n}.parquet
PYTHONPATH=src .venv/bin/python -m pqscan.cli run \
    --files fixtures/bench/files.json --queries fixtures/bench/queries.json --out output
# 2) 批处理结算：读取标记事实 + 程序合同
PYTHONPATH=src .venv/bin/python -m pqscan.cli settle --batch fixtures/bench/batch-ab.json
PYTHONPATH=src .venv/bin/python -m pqscan.cli settle --batch fixtures/bench/batch-cn.json
PYTHONPATH=src .venv/bin/python -m pqscan.cli settle --batch fixtures/bench/batch-ab-gap0.json
```

结算产物：`output/settlement/<batch_id>.parquet`（每行一步）与
`output/settlement/<batch_id>.json`（人读报告，含候选数、选中程序、各步证据与出处）。
样例覆盖：跨文件成段（D1 的 A@2 在 bench-a、B@3 在 bench-b）、未知值取反
（NULL 列表/NULL 元素/NULL 字段在取反查询下都不产出命中）、同刻多个采样
（D2 的 ts=10 与 ts=12 各有两条采样，结算时按采样号字典序取舍）。

预期现象：

| 批 | 合同 | 结果 |
| --- | --- | --- |
| `batch-ab` | A→B，相邻间隔 ≤3 | D1 选中 2 段：[2,3]（采样 101→102）、[4,5]（103→104），总跨度 2 |
| `batch-cn` | C→N，相邻间隔 ≤2 | D2 选中 1 段：[12,13]（203→204；同刻的另一个 C 采样 206 字典序落选） |
| `batch-ab-gap0` | A→B，相邻间隔 ≤0 | 严格递增时刻下间隔不可能 ≤0，一段也不能结算 |

去掉采样 103（A@4）后，`batch-ab` 只会结算 [2,3] 一段（对应需求示例的两个阶段）。

## 支持范围

- 平面 schema；整数（有/无符号 int32/int64 等）与字符串列；
- 比较 `eq/ne/lt/le/gt/ge`、`is_null`、`and/or/not`、列投影；
- 谓词以 JSON 前缀树表达（见 `fixtures/queries.json`），不解析 SQL；
- 类型不匹配、列不存在、嵌套 schema 等在计划期报错；不处理文件损坏。

## 嵌套探头读数标记

台架记录：一条 Parquet 记录是一台设备的一次台架采样，包含
`device`（字符串）、`ts`（整数时刻）、`sample_id`（唯一采样号）与
`readings`（有序探头读数列表，元素为 `struct<probe: string, value: int64>`；
元素或字段都可能为空）。列名可用查询里的 `"record"` 映射覆盖。

标记查询（`queries.json` 中含 `"exists"` 键的对象，与平面查询可混排）：

```json
{"id": "m_a", "marker": "A",
 "exists": [{"eq": ["probe", "P"]}, {"ge": ["value", 8]}]},
{"id": "m_n", "marker": "N", "negate": true,
 "exists": [{"eq": ["probe", "R"]}, {"ge": ["value", 100]}]}
```

- **存在语义（三值逻辑）**：整组条件在**同一个元素**上合取（组内只支持
  `eq/ne/lt/le/gt/ge/is_null`，不嵌套 and/or/not）。列表为 NULL 时结论未知；
  空表为假；非空表有真元素为真（证据 = 全部真元素的原始下标），
  无真元素且有未知元素为未知，全假为假。
  例：`[('P',2),('Q',9)]` 不能满足“探头 P 且值≥8”（不跨元素拼值）；
  `[NULL,('P',8)]` 命中下标 1。
- **取反**：`"negate": true` 对整个存在条件取反，仍只输出真；
  因取反而命中时证据下标为空。未知取反仍是未知，不会产出命中。
- **输出（标记事实）**：`results/<id>.parquet`，固定 schema 为
  `marker / device / ts / sample_id / hit_indices(list<int64>) /
  _source_file / _source_row_group / _source_row_in_group`，
  行序与平面查询一致（文件→行组→组内行号）。同一采样命中两种标记时
  每个标记各产一行（`sample_id` 相同、`marker` 不同），与两次不同采样可区分。
- **计划**：只用读数叶列的 footer 统计做**单条件证伪**——任一条件被自身那一列的
  min/max（`is_null` 用 `null_count=0`）证明没有任何元素能满足，则整组条件不可能
  为真、行组跳过；统计缺失/不可靠保守保留；绝不把不同列（不同元素）的极值拼成
  一次命中。取反查询无法仅凭 footer 证明“不存在确定为假的行”，全部保守保留。
- **执行**：两阶段列读取——先读 `readings` 求每行三值结论（PyArrow Kleene 内核，
  NULL 元素的结论被掩为未知），命中组才读 `device/ts/sample_id`；
  独立核对路径为无剪枝全扫描 + 纯 Python 3VL，与引擎不同代码路径。

## 台架程序批处理结算

`settle` 读取标记事实（上一步的 `results/*.parquet`）与生产线程序合同，
在每台设备上挑足尽可能多的互不重叠程序：

- **合同**：`{"steps": ["A","B"], "max_gaps": [3]}`——按顺序的标记名 +
  每相邻两步允许的最大时间差（边界值可用，即 `<=`）。单份合同最多 4 步，
  每批固定一个合同（批描述 JSON 见 `fixtures/bench/batch-*.json`，
  `marker_files` 相对批描述文件解析，也可用 `--markers` 覆盖）。
- **成段**：步与步必须来自同一设备上**严格递增时刻**的不同采样
  （同刻多个采样不能相接；同一采样不能充当两步）。
- **覆盖与冲突**：程序覆盖从首步到末步的闭时间区间 `[start,end]`；
  选中程序的闭区间互不重叠（前段末时刻必须严格小于后段首时刻）。
- **最优规则**：段数最多；并列先取总跨度（各段跨度之和）较小；
  再按各段依次使用的采样号序列字典序定胜负。
  实现为候选枚举 + 加权区间调度 DP（每台设备最多 60 个采样，超限报错）。
- **输出**：`settlement/<batch_id>.parquet`（每行一步：设备、程序序号、步序号、
  标记、时刻、采样号、证据下标、程序区间、物理出处）与
  `settlement/<batch_id>.json`（人读报告：候选数、选中程序、各步原始采样与证据）。
- 输入一致性：同一 `(device, sample_id)` 出现两个不同时刻视为矛盾输入，直接报错。

## 关键设计约束（与需求逐条对应）

1. **计划只读 footer。** `stats.py` 只访问 `FileMetaData/RowGroupMetaData/Statistics`，
   不调用任何数据读取接口。
2. **三值逻辑双结论。** 每个谓词子树求 `can_be_true`/`can_be_false`，
   NULL 产生的 UNKNOWN 既不算 TRUE 也不算 FALSE。`NOT` 同时翻转两个结论，
   不会出现“把 UNKNOWN 当 FALSE 再取反”的错误剪枝（见 `planner.py`）。
3. **统计缺失/不可靠即保守保留。** 无 Statistics、`has_min_max=False`（全 NULL 组的
   典型形态）、`null_count` 缺失、物理类型不受支持，叶子结论退化为
   `(true, true)` 并在 trace 的 `unknown` 中登记原因。PyArrow 19 的 Statistics
   不暴露 min/max 截断标记，因此对存在且类型受支持的 min/max 视为精确；
   若环境提供截断信息，应在 `stats.py` 将其降级为不可靠。
4. **AND/OR 不跨列关联极值。** 只组合独立子树的两个结论，不假设不同列的
   min/max 来自同一行。只有证明 `can_be_true=false` 才 `skip`。
5. **两阶段扫描。** 保留组先读过滤列；`Table.filter` 的 Kleene 语义丢弃
   UNKNOWN/FALSE、只留 TRUE，再用临时行号列取回组内物理行号。无命中组到此结束，
   不读 payload 等投影专用列；有命中组才读剩余投影列。过滤列同时在投影中时复用
   第一阶段数组，不会发起第二次读取（空列集合不产生请求）。
6. **读取计数来自执行入口。** `InstrumentedParquetFile.read_row_group` 在真实
   调用发生时登记 `(文件, 行组, phase, 列集合)`，计划器不参与填写；记录的是工具
   发起的列读取请求，不是操作系统字节数，也不用 footer 的压缩大小冒充 I/O。
7. **结果与溯源。** 输出按 files.json 文件顺序、行组物理索引、组内行号排列；
   附 `_source_file/_source_row_group/_source_row_in_group` 三列；重复数据保留；
   空结果写出同 schema 的零行 Parquet。
8. **独立核对。** `reference.py` 不看任何统计、每个行组全列全量读取，用纯 Python
   3VL 逐行求值，逐行比对引擎结果的内容、顺序与溯源。

## 夹具与现成查询的预期现象

两个 Parquet 由同一份 20 行数据写成，各 5 个行组、每组 4 行；RG2 的 `k` 全 NULL。

| 查询 | stats.parquet 计划 | no-stats.parquet 计划 |
| --- | --- | --- |
| `range`（k∈[14,30)） | 跳过 RG0/RG1/RG4；RG2 全 NULL 保守保留但过滤后无命中；仅 RG3 命中 | 5 组全保留，结果完全一致 |
| `nulls`（k IS NULL） | null_count=0 的 RG1/RG3/RG4 证明跳过；RG0/RG2 命中 | 全保留，结果一致 |
| `negated`（NOT kind='A'） | 全部保留（含 NULL 行，UNKNOWN 不取反为 TRUE） | 全保留，结果一致 |
| `or_missing_stats`（kind='A' OR amount>160） | 全部保留（OR 跨列独立推理） | 全保留，结果一致 |
| `no_hits`（k>1000） | RG0/RG1/RG3/RG4 证明跳过；RG2 统计缺失保守保留、过滤后无命中 | 全保留、全部无命中 |

可在 `output/run_record.json` 验证：`no_hits` 与 `range` 中所有
`no_hits_after_filter` 行组只有 `phase=filter` 的请求，从不读取投影专用列
（`row_id/payload/kind/amount`）；`planned_skip` 行组零请求。

## 测试覆盖

`tests/test_pqscan.py`（12 用例）覆盖：范围剪枝、全 NULL 组、IS NULL 的
null_count 推理、NOT 的 UNKNOWN 处理与可证明 FALSE、OR 跨列不关联极值、
无统计全保留、3VL 真值表、无命中不读投影列、过滤列复用、端到端与无剪枝
全扫描一致、有/无统计结果相同而计划保守度不同、空结果 schema 与输出顺序。

`tests/test_nested.py`（23 用例）覆盖：存在条件三值语义（NULL 列表/空表/
未知元素/全假）、取反只输出真且证据为空、需求示例行（`[('P',2),('Q',9)]`
不命中、`[NULL,('P',8)]` 命中下标 1）、Arrow 引擎与纯 Python 路径逐行一致
（含 NULL 元素下残留垃圾值的掩蔽、500 行确定性模糊对照）、单列统计证伪
（probe 列与 value 列分别证伪）、取反查询全保留、无统计保守保留、端到端
标记事实与溯源、同一采样命中两种标记 vs 两次不同采样、两阶段读取记录、
空结果 schema；结算的需求示例两阶段（无 A@4 只选 2→3、有 A@4 选 2→3 与
4→5、间隔 0 不结算）、间隔边界可取、闭区间端点相接即重叠、总跨度与采样号
字典序两级决胜、同刻不可相接、步数/采样数上限与矛盾输入报错、三份合同的
端到端批处理（跨文件成段、取反证据为空、同刻多采样取舍）。
