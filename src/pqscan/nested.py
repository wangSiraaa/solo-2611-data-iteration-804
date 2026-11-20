"""嵌套探头读数：标记查询的解析、校验、三值逻辑求值、footer 计划与两阶段扫描。

台架记录 schema（列名可用查询里的 "record" 覆盖，默认值如下）：
    device    string            设备
    ts        整数              整数时刻
    sample_id 整数              唯一采样号
    readings  list<struct>      有序探头读数列表；元素可为 NULL，元素字段可为 NULL

标记查询（queries.json 中含 "exists" 键的对象）：
    {"id": "m_a", "marker": "A",
     "exists": [{"eq": ["probe", "P"]}, {"ge": ["value", 8]}],
     "negate": false,                       # 可选，默认 false；为 true 时对整个存在条件取反
     "record": {"device": "...", "ts": "...", "sample_id": "...", "readings": "..."}}  # 可选

语义（SQL 三值逻辑，整组条件在同一个元素上合取）：
- 列表为 NULL -> 未知；空列表 -> 假；
- 非空列表：有真元素 -> 真（证据为全部真元素的原始下标）；
  无真元素但有未知元素 -> 未知；全假 -> 假。
- 取反只输出真：存在条件确定为假时命中，证据下标为空；未知取反仍未知，不输出。
- 条件组只在单个元素内合取，绝不把不同元素的取值（或列极值）拼成一次命中。

计划阶段只用 footer 中读数叶列的统计做“单条件证伪”：任一条件用**自己那一列**的
min/max（或 is_null 用 null_count=0）证明没有任何元素能满足，则整组条件不可能为真，
行组可跳过；统计缺失/不可靠一律保守保留。取反查询无法仅凭 footer 证明“不存在
确定为假的行”，全部保守保留。
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc

from .errors import PlanError
from .expr import COMPARE_OPS, Query, parse_queries
from .planner import GroupDecision, TraceNode
from .scanner import (
    PROVENANCE_FILE,
    PROVENANCE_RG,
    PROVENANCE_ROW,
    GroupScanRecord,
    InstrumentedParquetFile,
    QueryRun,
)
from .stats import ColumnStats, RowGroupStats, _column_stats

# 台架记录列名默认值（可用查询中的 "record" 映射覆盖）。
DEFAULT_RECORD = {"device": "device", "ts": "ts", "sample_id": "sample_id", "readings": "readings"}

# 标记事实（results/<id>.parquet）的固定列名：批处理按此契约读取。
FACT_MARKER = "marker"
FACT_DEVICE = "device"
FACT_TS = "ts"
FACT_SAMPLE = "sample_id"
FACT_HIT_INDICES = "hit_indices"

_PC_COMPARE = {
    "eq": "equal",
    "ne": "not_equal",
    "lt": "less",
    "le": "less_equal",
    "gt": "greater",
    "ge": "greater_equal",
}


@dataclass(frozen=True)
class Cond:
    """存在条件组中的一条叶子条件：元素字段上的比较或 is_null。"""

    op: str                       # eq/ne/lt/le/gt/ge/is_null
    field: str                    # 读数元素的字段名（如 probe / value）
    literal: Any = None

    def describe(self) -> str:
        if self.op == "is_null":
            return f"{self.field} IS NULL"
        return f"{self.field} {self.op} {self.literal!r}"


@dataclass(frozen=True)
class MarkerQuery:
    """一条标记查询：存在同一次读数满足整组条件（可整体取反）。"""

    id: str
    marker: str
    conds: tuple[Cond, ...]
    negate: bool
    record: dict[str, str]        # device/ts/sample_id/readings 的实际列名
    raw: dict[str, Any]           # 原始 JSON，用于计划文档回显


def is_marker_item(item: Any) -> bool:
    return isinstance(item, dict) and "exists" in item


def parse_query_item(item: dict[str, Any]) -> "Query | MarkerQuery":
    """把 queries.json 的一条查询解析为平面查询或标记查询。"""
    if is_marker_item(item):
        return parse_marker(item)
    return parse_queries([item])[0]


def parse_marker(item: dict[str, Any]) -> MarkerQuery:
    if not isinstance(item, dict) or "id" not in item:
        raise PlanError(f"标记查询必须是包含 id 的对象: {item!r}")
    marker = item.get("marker")
    if not isinstance(marker, str) or not marker:
        raise PlanError(f"标记查询 {item.get('id')!r} 需要非空字符串 marker")
    raw_conds = item.get("exists")
    if not isinstance(raw_conds, list) or not raw_conds:
        raise PlanError(f"标记查询 {item['id']} 的 exists 必须是非空条件列表")
    conds = tuple(_parse_cond(c, item["id"]) for c in raw_conds)
    negate = item.get("negate", False)
    if not isinstance(negate, bool):
        raise PlanError(f"标记查询 {item['id']} 的 negate 必须是布尔值")
    record = dict(DEFAULT_RECORD)
    custom = item.get("record", {})
    if not isinstance(custom, dict) or any(k not in DEFAULT_RECORD for k in custom):
        raise PlanError(
            f"标记查询 {item['id']} 的 record 只能包含 {sorted(DEFAULT_RECORD)} 的列名映射"
        )
    for key, col in custom.items():
        if not isinstance(col, str):
            raise PlanError(f"标记查询 {item['id']} 的 record.{key} 必须是列名字符串")
        record[key] = col
    if len(set(record.values())) != len(record):
        raise PlanError(f"标记查询 {item['id']} 的 record 列名映射存在重复")
    return MarkerQuery(
        id=str(item["id"]), marker=marker, conds=conds, negate=negate,
        record=record, raw=item,
    )


def _parse_cond(raw: Any, query_id: str) -> Cond:
    if not isinstance(raw, dict) or len(raw) != 1:
        raise PlanError(f"标记查询 {query_id} 的条件必须是单键对象: {raw!r}")
    (op, body), = raw.items()
    if op in COMPARE_OPS:
        if not isinstance(body, list) or len(body) != 2 or not isinstance(body[0], str):
            raise PlanError(f"标记查询 {query_id} 的 {op} 形式必须是 [字段名, 字面量]: {body!r}")
        return Cond(op=op, field=body[0], literal=body[1])
    if op == "is_null":
        if not isinstance(body, str):
            raise PlanError(f"标记查询 {query_id} 的 is_null 形式必须是字段名: {body!r}")
        return Cond(op=op, field=body)
    raise PlanError(
        f"标记查询 {query_id} 的条件组只支持 {'/'.join(COMPARE_OPS)}/is_null，"
        f"组内不嵌套 and/or/not（取反请用查询级 negate）: {op!r}"
    )


def _readings_struct(query: MarkerQuery, schema: pa.Schema) -> pa.StructType:
    """校验台架记录结构，返回读数元素的 struct 类型。"""
    rec = query.record
    for key in ("device", "ts", "sample_id", "readings"):
        if rec[key] not in schema.names:
            raise PlanError(f"标记查询 {query.id}: 记录缺少列 {rec[key]!r}")
    device_t = schema.field(rec["device"]).type
    if not (pa.types.is_string(device_t) or pa.types.is_large_string(device_t)):
        raise PlanError(f"标记查询 {query.id}: 设备列 {rec['device']} 的类型 {device_t} 不是字符串")
    for key in ("ts", "sample_id"):
        t = schema.field(rec[key]).type
        if not pa.types.is_integer(t):
            raise PlanError(f"标记查询 {query.id}: 列 {rec[key]} 的类型 {t} 不是整数")
    readings_t = schema.field(rec["readings"]).type
    if not (pa.types.is_list(readings_t) or pa.types.is_large_list(readings_t)):
        raise PlanError(f"标记查询 {query.id}: 读数列 {rec['readings']} 的类型 {readings_t} 不是列表")
    elem_t = readings_t.value_field.type
    if not pa.types.is_struct(elem_t):
        raise PlanError(f"标记查询 {query.id}: 读数元素类型 {elem_t} 不是 struct")
    return elem_t


def validate_marker(query: MarkerQuery, schema: pa.Schema) -> None:
    """对标记查询做结构、记录 schema 与条件字面量类型校验，不合法抛 PlanError。"""
    elem_t = _readings_struct(query, schema)
    for cond in query.conds:
        if cond.field not in elem_t.names:
            raise PlanError(f"标记查询 {query.id}: 读数元素没有字段 {cond.field!r}")
        if cond.op == "is_null":
            continue
        field_t = elem_t.field(cond.field).type
        lit = cond.literal
        if pa.types.is_integer(field_t):
            if isinstance(lit, bool) or not isinstance(lit, int):
                raise PlanError(
                    f"标记查询 {query.id}: 整数字段 {cond.field} 不能与字面量 {lit!r} 比较"
                )
        elif pa.types.is_string(field_t) or pa.types.is_large_string(field_t):
            if not isinstance(lit, str):
                raise PlanError(
                    f"标记查询 {query.id}: 字符串字段 {cond.field} 不能与字面量 {lit!r} 比较"
                )
        else:
            raise PlanError(
                f"标记查询 {query.id}: 字段 {cond.field} 的类型 {field_t} 不支持比较（仅整数/字符串）"
            )


# ---------------------------------------------------------------------------
# 纯 Python 三值逻辑求值（供参考全扫描核对；True/False/None 对应 TRUE/FALSE/UNKNOWN）
# ---------------------------------------------------------------------------

def eval_cond(cond: Cond, element: dict[str, Any] | None) -> bool | None:
    """单条条件在单个元素上的三值结果；元素为 NULL 时整组条件为 UNKNOWN。"""
    if element is None:
        return None
    value = element[cond.field]
    if cond.op == "is_null":
        return value is None
    if value is None:
        return None
    if cond.op == "eq":
        return bool(value == cond.literal)
    if cond.op == "ne":
        return bool(value != cond.literal)
    if cond.op == "lt":
        return bool(value < cond.literal)
    if cond.op == "le":
        return bool(value <= cond.literal)
    if cond.op == "gt":
        return bool(value > cond.literal)
    return bool(value >= cond.literal)  # ge


def eval_element(conds: tuple[Cond, ...], element: dict[str, Any] | None) -> bool | None:
    """整组条件在同一个元素上合取（Kleene AND），不跨元素组合取值。"""
    if element is None:
        return None
    saw_unknown = False
    for cond in conds:
        r = eval_cond(cond, element)
        if r is False:
            return False
        if r is None:
            saw_unknown = True
    return None if saw_unknown else True


def eval_exists(conds: tuple[Cond, ...], readings: list[Any] | None) -> tuple[bool | None, list[int]]:
    """存在条件的三值结果与真元素下标。

    返回 (三值结论, 真元素原始下标)。列表 NULL -> (None, [])；空列表 -> (False, [])；
    有真元素 -> (True, 全部真元素下标)；无真但有未知 -> (None, [])；全假 -> (False, [])。
    """
    if readings is None:
        return None, []
    if len(readings) == 0:
        return False, []
    hits: list[int] = []
    saw_unknown = False
    for i, element in enumerate(readings):
        r = eval_element(conds, element)
        if r is True:
            hits.append(i)
        elif r is None:
            saw_unknown = True
    if hits:
        return True, hits
    if saw_unknown:
        return None, []
    return False, []


def eval_marker(query: MarkerQuery, readings: list[Any] | None) -> tuple[bool, list[int]]:
    """标记级结论：只输出真。返回 (是否命中, 证据下标)；取反而命中时证据为空。"""
    truth, hits = eval_exists(query.conds, readings)
    if query.negate:
        return (truth is False), []
    return (truth is True), (hits if truth is True else [])


# ---------------------------------------------------------------------------
# 执行引擎：PyArrow compute 求值（与上面纯 Python 路径相互独立）
# ---------------------------------------------------------------------------

def _cond_arrow(cond: Cond, arr: pa.Array) -> pa.BooleanArray:
    if cond.op == "is_null":
        return pc.is_null(arr)
    fn = getattr(pc, _PC_COMPARE[cond.op])
    return fn(arr, cond.literal)


def exists_arrow(readings: pa.ListArray, conds: tuple[Cond, ...]) -> tuple[list[bool | None], list[list[int]]]:
    """对整列读数列表求存在条件：返回每行的三值结论与真元素下标。

    条件用 PyArrow 的 Kleene 内核在“打平的元素数组”上向量化求值并合取；
    再按元素所属行归约出每行结论。行内归约只统计真/未知元素，
    不会把不同元素的取值拼成一次命中。
    """
    n = len(readings)
    assert readings.offset == 0, "读数列必须先 combine_chunks 归零偏移"
    lengths = pc.list_value_length(readings).to_pylist()  # NULL 列表 -> None
    flat = pc.list_flatten(readings)                      # StructArray，保留 NULL 元素
    parents = pc.list_parent_indices(readings).to_pylist()

    state: pa.BooleanArray | None = None
    for cond in conds:
        r = _cond_arrow(cond, flat.field(cond.field))
        state = r if state is None else pc.and_kleene(state, r)
    # NULL 元素（整个 struct 为空）时子字段缓冲区的取值无意义（可能是任意非 NULL 值），
    # 必须把该元素的三值结论掩为 UNKNOWN，与纯 Python 路径的“元素为空则未知”一致。
    state = pc.if_else(pc.is_valid(flat), state, pa.scalar(None, type=pa.bool_()))

    t_flags = pc.fill_null(pc.equal(state, True), False).to_pylist()
    f_flags = pc.fill_null(pc.equal(state, False), False).to_pylist()
    offsets = readings.offsets.to_pylist()

    truths: list[bool | None] = [None] * n
    evidence: list[list[int]] = [[] for _ in range(n)]
    has_true = [False] * n
    has_unknown = [False] * n
    for k, p in enumerate(parents):
        if t_flags[k]:
            has_true[p] = True
            evidence[p].append(k - offsets[p])  # 元素在原列表中的原始下标
        elif not f_flags[k]:
            has_unknown[p] = True
    for i in range(n):
        if lengths[i] is None:
            truths[i] = None                    # 列表为 NULL -> 未知
        elif has_true[i]:
            truths[i] = True
        elif lengths[i] == 0:
            truths[i] = False                   # 空列表 -> 假
        elif has_unknown[i]:
            truths[i] = None                    # 无真元素但有未知元素 -> 未知
        else:
            truths[i] = False
    return truths, evidence


def marker_hits_arrow(
    readings: pa.ListArray, conds: tuple[Cond, ...], negate: bool
) -> tuple[list[int], list[list[int]]]:
    """返回命中行号与对应证据下标；取反而命中时证据为空列表。"""
    truths, evidence = exists_arrow(readings, conds)
    rows: list[int] = []
    out_evidence: list[list[int]] = []
    for i, truth in enumerate(truths):
        if negate:
            if truth is False:
                rows.append(i)
                out_evidence.append([])
        elif truth is True:
            rows.append(i)
            out_evidence.append(evidence[i])
    return rows, out_evidence


# ---------------------------------------------------------------------------
# 计划阶段：只用 footer 的读数叶列统计做单条件证伪
# ---------------------------------------------------------------------------

def _leaf_parquet_path(query: MarkerQuery, schema: pa.Schema, field_name: str) -> str:
    """读数元素字段对应的 Parquet 叶列路径（三级 list 编码）。"""
    readings_t = schema.field(query.record["readings"]).type
    return f"{query.record['readings']}.list.{readings_t.value_field.name}.{field_name}"


def _resolve_leaf_paths(
    query: MarkerQuery, schema: pa.Schema, footer_paths: list[str], path: str
) -> dict[str, str]:
    """把每个条件字段解析到 footer 中的叶列路径。

    先按 Arrow 三级 list 编码的约定路径精确匹配；失败时退化为
    “以读数列开头、以字段名结尾”的唯一后缀匹配；仍不唯一则报错。
    """
    wanted: dict[str, str] = {}
    for cond in query.conds:
        if cond.field in wanted:
            continue
        expected = _leaf_parquet_path(query, schema, cond.field)
        if expected in footer_paths:
            wanted[cond.field] = expected
            continue
        prefix = f"{query.record['readings']}."
        suffix = f".{cond.field}"
        candidates = [p for p in footer_paths if p.startswith(prefix) and p.endswith(suffix)]
        if len(candidates) != 1:
            raise PlanError(
                f"{path}: footer 中无法唯一确定读数字段 {cond.field!r} 的叶列"
                f"（候选: {candidates or '无'}）"
            )
        wanted[cond.field] = candidates[0]
    return wanted


def read_nested_footer_stats(
    path: str, schema: pa.Schema, query: MarkerQuery
) -> list[RowGroupStats]:
    """只读 footer，提取查询涉及的条件字段所在叶列的行组统计。

    返回的 RowGroupStats.columns 以**元素字段名**为键，只包含条件引用的字段；
    与平面 stats.read_footer_stats 一样只访问 FileMetaData/RowGroupMetaData/Statistics。
    """
    import pyarrow.parquet as pq

    validate_marker(query, schema)
    pf = pq.ParquetFile(path)
    if pf.num_row_groups == 0:
        return []
    footer_paths = [
        pf.metadata.row_group(0).column(ci).path_in_schema
        for ci in range(pf.metadata.row_group(0).num_columns)
    ]
    wanted = _resolve_leaf_paths(query, schema, footer_paths, path)
    groups: list[RowGroupStats] = []
    for rgi in range(pf.num_row_groups):
        rg_meta = pf.metadata.row_group(rgi)
        by_path = {
            rg_meta.column(ci).path_in_schema: rg_meta.column(ci)
            for ci in range(rg_meta.num_columns)
        }
        cols: dict[str, ColumnStats] = {}
        for field_name, leaf_path in wanted.items():
            col_meta = by_path.get(leaf_path)
            if col_meta is None:
                raise PlanError(f"{path}: footer 中找不到读数叶列 {leaf_path!r}")
            cols[field_name] = _column_stats(col_meta, rg_meta.num_rows)
        groups.append(RowGroupStats(rgi, rg_meta.num_rows, cols))
    return groups


def _leaf_can_be_true(cond: Cond, cs: ColumnStats) -> tuple[bool, str | None]:
    """用单列统计判断“行组内是否可能有元素满足该条件”；只证伪，不证实。

    返回 (can_be_true, 保守退化原因)。统计缺失/不可靠时无法排除任何取值，保守为真。
    """
    if cond.op == "is_null":
        if not cs.null_count_reliable:
            return True, f"字段 {cond.field} 的 null_count 缺失，结论退化为 UNKNOWN 保守保留"
        return (cs.null_count or 0) > 0, None
    if not cs.min_max_reliable:
        return True, f"字段 {cond.field} 的 min/max 不可靠，结论退化为 UNKNOWN 保守保留"
    lo, hi = cs.min, cs.max
    lit = cond.literal
    if cond.op == "eq":
        return lo <= lit <= hi, None
    if cond.op == "ne":
        return lo != lit or hi != lit, None
    if cond.op == "lt":
        return lo < lit, None
    if cond.op == "le":
        return lo <= lit, None
    if cond.op == "gt":
        return hi > lit, None
    return hi >= lit, None  # ge


def _stats_snapshot(cs: ColumnStats) -> dict[str, Any]:
    snap: dict[str, Any] = {
        "num_rows": cs.num_rows,
        "null_count": cs.null_count,
        "min": cs.min,
        "max": cs.max,
        "min_max_reliable": cs.min_max_reliable,
        "null_count_reliable": cs.null_count_reliable,
    }
    if cs.unknown_reasons:
        snap["unknown_reasons"] = list(cs.unknown_reasons)
    return snap


def plan_marker_file(
    path: str, query: MarkerQuery, groups: list[RowGroupStats]
) -> list[GroupDecision]:
    """标记查询的行组计划。

    正存在条件：任一条件被自身叶列统计证伪 -> 整组条件不可能为真 -> skip；
    否则保守保留。取反查询：footer 无法证明“不存在确定为假的行”，全部保留。
    """
    decisions: list[GroupDecision] = []
    for rg in groups:
        children: list[TraceNode] = []
        missing: list[str] = []
        can_be_true = True
        for cond in query.conds:
            cs = rg.columns[cond.field]
            leaf_true, degrade = _leaf_can_be_true(cond, cs)
            if degrade:
                missing.append(cond.field)
            children.append(TraceNode(
                op=cond.op, detail=cond.describe(),
                can_be_true=leaf_true, can_be_false=True,
                stats_used={cond.field: _stats_snapshot(cs)},
                unknown=[degrade] if degrade else [],
            ))
            can_be_true = can_be_true and leaf_true

        detail = " AND ".join(c.describe() for c in query.conds)
        if query.negate:
            # NOT(EXISTS ...)：命中要求某行的存在条件确定为假；footer 统计无法
            # 证明“组内不存在这样的行”，因此始终保守保留。
            trace = TraceNode(
                op="not", detail=f"NOT EXISTS ({detail})",
                can_be_true=True, can_be_false=can_be_true,
                unknown=["取反查询无法仅凭 footer 统计排除“存在条件确定为假的行”，保守保留"],
                children=children,
            )
            decisions.append(GroupDecision(
                file=path, row_group=rg.row_group_index, num_rows=rg.num_rows,
                action="keep",
                reason="保留：取反的存在条件无法仅凭 footer 统计证伪，保守保留",
                can_be_true=True, can_be_false=can_be_true,
                trace=trace, missing_stats=tuple(sorted(set(missing))),
            ))
            continue

        trace = TraceNode(
            op="exists", detail=f"EXISTS ({detail})",
            can_be_true=can_be_true, can_be_false=True, children=children,
        )
        if can_be_true:
            if missing:
                reason = (
                    f"保留：统计缺失/不可靠（{', '.join(sorted(set(missing)))}），"
                    f"无法证明不存在 TRUE 行，保守保留"
                )
            else:
                reason = "保留：各条件均无法被自身叶列统计证伪，需扫描精确定位"
            action = "keep"
        else:
            reason = "跳过：至少一个条件被自身叶列统计证明没有任何元素能满足，整组条件不可能为真"
            action = "skip"
        decisions.append(GroupDecision(
            file=path, row_group=rg.row_group_index, num_rows=rg.num_rows,
            action=action, reason=reason,
            can_be_true=can_be_true, can_be_false=True,
            trace=trace, missing_stats=tuple(sorted(set(missing))),
        ))
    return decisions


# ---------------------------------------------------------------------------
# 执行阶段：两阶段列读取（先读读数列过滤，命中组才读设备/时刻/采样号）
# ---------------------------------------------------------------------------

def fact_schema(device_t: pa.DataType, ts_t: pa.DataType, sample_t: pa.DataType) -> pa.Schema:
    """标记事实 Parquet 的固定 schema（批处理按此契约读取）。"""
    return pa.schema([
        pa.field(FACT_MARKER, pa.string()),
        pa.field(FACT_DEVICE, device_t),
        pa.field(FACT_TS, ts_t),
        pa.field(FACT_SAMPLE, sample_t),
        pa.field(FACT_HIT_INDICES, pa.list_(pa.int64())),
        pa.field(PROVENANCE_FILE, pa.string()),
        pa.field(PROVENANCE_RG, pa.int32()),
        pa.field(PROVENANCE_ROW, pa.int64()),
    ])


def _fact_schema_for(query: MarkerQuery, schema: pa.Schema) -> pa.Schema:
    rec = query.record
    return fact_schema(
        schema.field(rec["device"]).type,
        schema.field(rec["ts"]).type,
        schema.field(rec["sample_id"]).type,
    )


def scan_marker_query(
    query: MarkerQuery,
    decisions_by_file: dict[str, list[GroupDecision]],
    reader_factory: type[InstrumentedParquetFile],
    output_path: str,
) -> QueryRun:
    """执行一条标记查询并写出标记事实 Parquet（schema 见 fact_schema）。

    行序与平面查询一致：文件顺序 -> 行组物理索引 -> 组内物理行号；
    同一采样命中多个标记时，每个标记查询各产出一行（靠 sample_id 与 marker 区分）。
    """
    import pyarrow.parquet as pq

    rec = query.record
    filter_cols = (rec["readings"],)
    projection_only = (rec["device"], rec["ts"], rec["sample_id"])

    readers = {path: reader_factory(path) for path in decisions_by_file}
    group_records: list[GroupScanRecord] = []
    batches: list[pa.Table] = []
    total_rows = 0

    for file, decisions in decisions_by_file.items():
        reader = readers[file]
        schema = reader.schema_arrow
        out_schema = _fact_schema_for(query, schema)
        for decision in decisions:
            if decision.action == "skip":
                group_records.append(GroupScanRecord(
                    file=file, row_group=decision.row_group, num_rows=decision.num_rows,
                    status="planned_skip", matched_rows=0,
                    filter_columns=filter_cols, projection_only_columns=projection_only,
                    note="计划阶段已证明整组条件不可能为真，执行阶段不发起任何列读取",
                ))
                continue

            rgi = decision.row_group
            # 阶段 1：只读读数列，求每行三值结论与证据下标。
            before = len(reader.requests)
            filter_tbl = reader.read_row_group(rgi, list(filter_cols), phase="filter")
            filter_reads = list(reader.requests[before:])

            readings = filter_tbl.column(rec["readings"]).combine_chunks()
            hit_rows, hit_evidence = marker_hits_arrow(readings, query.conds, query.negate)

            if not hit_rows:
                group_records.append(GroupScanRecord(
                    file=file, row_group=rgi, num_rows=decision.num_rows,
                    status="no_hits_after_filter", matched_rows=0,
                    filter_columns=filter_cols, projection_only_columns=projection_only,
                    column_reads=filter_reads,
                    note="过滤后无命中行，不读取任何投影专用列",
                ))
                continue

            # 阶段 2：只为命中组读取设备/时刻/采样号。
            before = len(reader.requests)
            payload_tbl = reader.read_row_group(rgi, list(projection_only), phase="projection")
            payload_reads = list(reader.requests[before:])

            take = pa.array(hit_rows)
            n = len(hit_rows)
            arrays = {
                FACT_MARKER: pa.array([query.marker] * n, type=pa.string()),
                FACT_DEVICE: payload_tbl.column(rec["device"]).take(take),
                FACT_TS: payload_tbl.column(rec["ts"]).take(take),
                FACT_SAMPLE: payload_tbl.column(rec["sample_id"]).take(take),
                FACT_HIT_INDICES: pa.array(hit_evidence, type=pa.list_(pa.int64())),
                PROVENANCE_FILE: pa.array([file] * n, type=pa.string()),
                PROVENANCE_RG: pa.array([rgi] * n, type=pa.int32()),
                PROVENANCE_ROW: pa.array(hit_rows, type=pa.int64()),
            }
            batches.append(pa.table(arrays, schema=out_schema))
            total_rows += n
            group_records.append(GroupScanRecord(
                file=file, row_group=rgi, num_rows=decision.num_rows,
                status="matched", matched_rows=n,
                filter_columns=filter_cols, projection_only_columns=projection_only,
                column_reads=filter_reads + payload_reads,
                note="先读读数列求三值结论，命中后再读设备/时刻/采样号",
            ))

    if batches:
        result = pa.concat_tables(batches, promote_options="none")
    else:
        first_reader = next(iter(readers.values()))
        out_schema = _fact_schema_for(query, first_reader.schema_arrow)
        result = pa.table(
            {f.name: pa.array([], type=f.type) for f in out_schema}, schema=out_schema
        )

    pq.write_table(result, output_path, write_statistics=True)
    all_requests = [req for reader in readers.values() for req in reader.requests]
    return QueryRun(
        query_id=query.id, output_path=output_path, result_rows=total_rows,
        group_records=group_records, all_read_requests=all_requests,
    )


# ---------------------------------------------------------------------------
# 独立核对：无剪枝全扫描 + 纯 Python 三值逻辑（与 exists_arrow 不同代码路径）
# ---------------------------------------------------------------------------

def reference_marker_scan(query: MarkerQuery, files: list[str]) -> pa.Table:
    """不看任何统计、每个行组全列全量读取，逐行用纯 Python 3VL 求值。"""
    import pyarrow.parquet as pq

    rec = query.record
    batches: list[pa.Table] = []
    for path in files:
        pf = pq.ParquetFile(path)
        schema = pf.schema_arrow
        out_schema = _fact_schema_for(query, schema)
        for rgi in range(pf.num_row_groups):
            tbl = pf.read_row_group(rgi)  # 全列、全量、无统计推理
            rows = tbl.to_pylist()
            hits: list[int] = []
            evidence: list[list[int]] = []
            for i, row in enumerate(rows):
                hit, idx = eval_marker(query, row[rec["readings"]])
                if hit:
                    hits.append(i)
                    evidence.append(idx)
            if not hits:
                continue
            take = pa.array(hits)
            n = len(hits)
            arrays = {
                FACT_MARKER: pa.array([query.marker] * n, type=pa.string()),
                FACT_DEVICE: tbl.column(rec["device"]).take(take),
                FACT_TS: tbl.column(rec["ts"]).take(take),
                FACT_SAMPLE: tbl.column(rec["sample_id"]).take(take),
                FACT_HIT_INDICES: pa.array(evidence, type=pa.list_(pa.int64())),
                PROVENANCE_FILE: pa.array([path] * n, type=pa.string()),
                PROVENANCE_RG: pa.array([rgi] * n, type=pa.int32()),
                PROVENANCE_ROW: pa.array(hits, type=pa.int64()),
            }
            batches.append(pa.table(arrays, schema=out_schema))

    if not batches:
        any_schema = pq.ParquetFile(files[0]).schema_arrow
        out_schema = _fact_schema_for(query, any_schema)
        return pa.table(
            {f.name: pa.array([], type=f.type) for f in out_schema}, schema=out_schema
        )
    return pa.concat_tables(batches, promote_options="none")
