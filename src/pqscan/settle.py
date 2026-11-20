"""台架程序批处理结算：读取标记事实 Parquet 与生产线程序合同，挑选互不重叠程序。

规则（与需求逐条对应）：
- 一个程序 = 按顺序的标记名 + 每相邻两步允许的最大时间差（边界值可用，即 <=）；
- 步与步必须来自同一设备上**严格递增时刻**的不同采样（同刻的多个采样不能相接）；
- 程序覆盖从首步到末步的闭时间区间 [start_ts, end_ts]，选中的程序区间互不重叠
  （闭区间：前一段的末时刻必须严格小于后一段的首时刻）；
- 每台设备上挑足尽可能多段；数量并列先取总跨度（各段跨度之和）较小的，
  再按各段依次使用的采样号序列的字典序定胜负；
- 单份合同最多 4 步，每台设备最多 60 个采样，每批固定一个合同。

输入标记事实的 schema 即 nested.fact_schema 的固定契约（marker/device/ts/sample_id/
hit_indices + 溯源三列）。同一 (device, sample_id) 出现不同时刻属于矛盾输入，直接报错。
"""
from __future__ import annotations

import bisect
import json
import os
from dataclasses import dataclass
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from .errors import PlanError
from .nested import FACT_DEVICE, FACT_HIT_INDICES, FACT_MARKER, FACT_SAMPLE, FACT_TS
from .scanner import PROVENANCE_FILE, PROVENANCE_RG, PROVENANCE_ROW

MAX_STEPS = 4
MAX_SAMPLES_PER_DEVICE = 60


@dataclass(frozen=True)
class Fact:
    """一条标记事实：某次采样命中了某个标记（含证据下标与物理出处）。"""

    device: str
    ts: int
    sample_id: int
    marker: str
    hit_indices: tuple[int, ...]
    src_file: str
    src_rg: int
    src_row: int


@dataclass(frozen=True)
class Contract:
    """程序合同：按顺序的标记名 + 每相邻两步允许的最大时间差。"""

    steps: tuple[str, ...]
    max_gaps: tuple[int, ...]


@dataclass(frozen=True)
class Program:
    """一段可结算程序：按合同顺序排列的各步标记事实。"""

    facts: tuple[Fact, ...]

    @property
    def start_ts(self) -> int:
        return self.facts[0].ts

    @property
    def end_ts(self) -> int:
        return self.facts[-1].ts

    @property
    def span(self) -> int:
        return self.end_ts - self.start_ts

    @property
    def sample_seq(self) -> tuple[int, ...]:
        return tuple(f.sample_id for f in self.facts)


# ---------------------------------------------------------------------------
# 输入读取与校验
# ---------------------------------------------------------------------------

def validate_contract(raw: Any) -> Contract:
    if not isinstance(raw, dict):
        raise PlanError(f"合同必须是对象: {raw!r}")
    steps = raw.get("steps")
    if (
        not isinstance(steps, list)
        or not 1 <= len(steps) <= MAX_STEPS
        or not all(isinstance(s, str) and s for s in steps)
    ):
        raise PlanError(f"合同 steps 必须是 1..{MAX_STEPS} 个非空标记名: {steps!r}")
    max_gaps = raw.get("max_gaps")
    if not isinstance(max_gaps, list) or len(max_gaps) != len(steps) - 1:
        raise PlanError(
            f"合同 max_gaps 必须是 {len(steps) - 1} 个相邻步时间差: {max_gaps!r}"
        )
    for gap in max_gaps:
        if isinstance(gap, bool) or not isinstance(gap, int) or gap < 0:
            raise PlanError(f"合同 max_gaps 的元素必须是非负整数: {gap!r}")
    return Contract(steps=tuple(steps), max_gaps=tuple(max_gaps))


def load_batch(path: str) -> tuple[str, Contract, list[str]]:
    """读取批描述 JSON：{"id", "contract": {...}, "marker_files": [...]}。

    marker_files 相对批描述文件所在目录解析；调用方可用 --markers 覆盖。
    """
    with open(path, encoding="utf-8") as fh:
        raw = json.load(fh)
    if not isinstance(raw, dict) or "contract" not in raw:
        raise PlanError(f"批描述必须包含 id/contract: {path}")
    batch_id = str(raw.get("id") or os.path.splitext(os.path.basename(path))[0])
    contract = validate_contract(raw["contract"])
    base = os.path.dirname(os.path.abspath(path))
    marker_files = [
        os.path.normpath(os.path.join(base, p)) for p in raw.get("marker_files", [])
    ]
    return batch_id, contract, marker_files


def load_facts(paths: list[str]) -> list[Fact]:
    """读取标记事实 Parquet，校验固定 schema，并做一致性检查。

    - 完全重复的事实（同设备/标记/采样号/时刻）只保留第一份；
    - 同一 (device, sample_id) 出现不同时刻 -> 矛盾输入，报错。
    """
    facts: list[Fact] = []
    seen: set[tuple[str, str, int, int]] = set()
    sample_ts: dict[tuple[str, int], int] = {}
    for path in paths:
        tbl = pq.read_table(path)
        _check_fact_schema(path, tbl.schema)
        for row in tbl.to_pylist():
            if any(row[c] is None for c in (FACT_DEVICE, FACT_MARKER, FACT_SAMPLE, FACT_TS)):
                raise PlanError(f"{path}: 标记事实的设备/标记/采样号/时刻不允许为空: {row!r}")
            key = (row[FACT_DEVICE], row[FACT_MARKER], row[FACT_SAMPLE], row[FACT_TS])
            if key in seen:
                continue
            seen.add(key)
            sample_key = (row[FACT_DEVICE], row[FACT_SAMPLE])
            if sample_key in sample_ts and sample_ts[sample_key] != row[FACT_TS]:
                raise PlanError(
                    f"采样号 {row[FACT_SAMPLE]}（设备 {row[FACT_DEVICE]}）"
                    f"出现两个不同时刻 {sample_ts[sample_key]} 与 {row[FACT_TS]}，输入矛盾"
                )
            sample_ts[sample_key] = row[FACT_TS]
            facts.append(Fact(
                device=row[FACT_DEVICE],
                ts=row[FACT_TS],
                sample_id=row[FACT_SAMPLE],
                marker=row[FACT_MARKER],
                hit_indices=tuple(row[FACT_HIT_INDICES] or ()),
                src_file=row[PROVENANCE_FILE],
                src_rg=row[PROVENANCE_RG],
                src_row=row[PROVENANCE_ROW],
            ))
    return facts


def _check_fact_schema(path: str, schema: pa.Schema) -> None:
    expect = {
        FACT_MARKER: lambda t: pa.types.is_string(t) or pa.types.is_large_string(t),
        FACT_DEVICE: lambda t: pa.types.is_string(t) or pa.types.is_large_string(t),
        FACT_TS: pa.types.is_integer,
        FACT_SAMPLE: pa.types.is_integer,
        FACT_HIT_INDICES: lambda t: pa.types.is_list(t) or pa.types.is_large_list(t),
        PROVENANCE_FILE: lambda t: pa.types.is_string(t) or pa.types.is_large_string(t),
        PROVENANCE_RG: pa.types.is_integer,
        PROVENANCE_ROW: pa.types.is_integer,
    }
    for name, ok in expect.items():
        if name not in schema.names:
            raise PlanError(f"{path}: 标记事实缺少列 {name!r}（应为 nested.fact_schema 契约）")
        if not ok(schema.field(name).type):
            raise PlanError(f"{path}: 列 {name} 的类型 {schema.field(name).type} 不符合标记事实契约")


# ---------------------------------------------------------------------------
# 每台设备的程序挑选
# ---------------------------------------------------------------------------

def enumerate_programs(facts: list[Fact], contract: Contract) -> list[Program]:
    """枚举一台设备上满足合同的全部候选程序（严格递增时刻、不同采样、间隔约束）。"""
    by_marker: dict[str, list[Fact]] = {m: [] for m in contract.steps}
    for fact in facts:
        if fact.marker in by_marker:
            by_marker[fact.marker].append(fact)
    for lst in by_marker.values():
        lst.sort(key=lambda f: (f.ts, f.sample_id))

    k = len(contract.steps)
    candidates: list[Program] = []
    chosen: list[Fact] = []

    def dfs(step: int, prev_ts: int | None, used_samples: frozenset[int]) -> None:
        if step == k:
            candidates.append(Program(facts=tuple(chosen)))
            return
        for fact in by_marker[contract.steps[step]]:
            if prev_ts is not None:
                if fact.ts <= prev_ts:
                    continue                      # 时刻必须严格递增
                if fact.ts - prev_ts > contract.max_gaps[step - 1]:
                    break                         # 列表按时刻升序，之后的只会更差
            if fact.sample_id in used_samples:
                continue                          # 步与步必须来自不同采样
            chosen.append(fact)
            dfs(step + 1, fact.ts, used_samples | {fact.sample_id})
            chosen.pop()

    dfs(0, None, frozenset())
    candidates.sort(key=lambda p: (p.end_ts, p.start_ts, p.sample_seq))
    return candidates


@dataclass(frozen=True)
class _Sol:
    """动态规划状态：已选程序数、总跨度、采样号序列、程序列表。"""

    count: int
    span: int
    seq: tuple[int, ...]
    programs: tuple[Program, ...]


_EMPTY = _Sol(count=0, span=0, seq=(), programs=())


def _better(a: _Sol, b: _Sol) -> bool:
    """a 是否严格优于 b：数量多者优先；并列先总跨度小，再采样号序列字典序小。"""
    if a.count != b.count:
        return a.count > b.count
    if a.span != b.span:
        return a.span < b.span
    return a.seq < b.seq


def select_programs(candidates: list[Program]) -> list[Program]:
    """加权区间调度：在互不重叠（闭区间）约束下按三档规则挑最优组合。"""
    if not candidates:
        return []
    ends = sorted({p.end_ts for p in candidates})
    by_end: dict[int, list[Program]] = {t: [] for t in ends}
    for p in candidates:
        by_end[p.end_ts].append(p)

    # dp[i] = 只使用末时刻 <= ends[i] 的程序时的最优解
    dp: list[_Sol] = []
    for i, end_ts in enumerate(ends):
        best = dp[i - 1] if i > 0 else _EMPTY
        for p in by_end[end_ts]:
            # 闭区间互不重叠：上一段末时刻必须严格小于本段首时刻
            j = bisect.bisect_left(ends, p.start_ts) - 1
            base = dp[j] if j >= 0 else _EMPTY
            cand = _Sol(
                count=base.count + 1,
                span=base.span + p.span,
                seq=base.seq + p.sample_seq,
                programs=base.programs + (p,),
            )
            if _better(cand, best):
                best = cand
        dp.append(best)
    return list(dp[-1].programs)


def settle_device(facts: list[Fact], contract: Contract) -> tuple[list[Program], int]:
    """单台设备：返回 (选中程序, 候选程序数)；采样数超限直接报错。"""
    n_samples = len({f.sample_id for f in facts})
    if n_samples > MAX_SAMPLES_PER_DEVICE:
        raise PlanError(
            f"设备 {facts[0].device} 有 {n_samples} 个采样，"
            f"超出每设备 {MAX_SAMPLES_PER_DEVICE} 个采样的批处理上限"
        )
    candidates = enumerate_programs(facts, contract)
    return select_programs(candidates), len(candidates)


# ---------------------------------------------------------------------------
# 批处理入口：结算 + 落盘
# ---------------------------------------------------------------------------

def settle_batch(
    facts: list[Fact], contract: Contract
) -> dict[str, list[Program]]:
    """按设备分组结算，返回 {设备: 选中程序}（程序按首时刻升序）。"""
    by_device: dict[str, list[Fact]] = {}
    for fact in facts:
        by_device.setdefault(fact.device, []).append(fact)
    result: dict[str, list[Program]] = {}
    for device in sorted(by_device):
        programs, _ = settle_device(by_device[device], contract)
        result[device] = programs
    return result


def settlement_schema() -> pa.Schema:
    """结算结果 Parquet 的 schema：每行是某选中程序的一步。"""
    return pa.schema([
        pa.field("device", pa.string()),
        pa.field("program_index", pa.int32()),
        pa.field("step_index", pa.int32()),
        pa.field("marker", pa.string()),
        pa.field("ts", pa.int64()),
        pa.field("sample_id", pa.int64()),
        pa.field("hit_indices", pa.list_(pa.int64())),
        pa.field("program_span", pa.int64()),
        pa.field("program_start_ts", pa.int64()),
        pa.field("program_end_ts", pa.int64()),
        pa.field(PROVENANCE_FILE, pa.string()),
        pa.field(PROVENANCE_RG, pa.int32()),
        pa.field(PROVENANCE_ROW, pa.int64()),
    ])


def _program_to_dict(program: Program) -> dict[str, Any]:
    return {
        "start_ts": program.start_ts,
        "end_ts": program.end_ts,
        "span": program.span,
        "steps": [
            {
                "marker": f.marker,
                "ts": f.ts,
                "sample_id": f.sample_id,
                "hit_indices": list(f.hit_indices),
                "source": {
                    "file": f.src_file,
                    "row_group": f.src_rg,
                    "row_in_group": f.src_row,
                },
            }
            for f in program.facts
        ],
    }


def run_settlement(
    batch_json: str, marker_files: list[str] | None, out_dir: str
) -> dict[str, Any]:
    """读取批描述与标记事实，结算并写出 settlement/<batch_id>.{parquet,json}。"""
    batch_id, contract, batch_markers = load_batch(batch_json)
    paths = marker_files if marker_files else batch_markers
    if not paths:
        raise PlanError("批描述没有 marker_files，且未通过 --markers 指定标记事实文件")
    facts = load_facts(paths)

    by_device: dict[str, list[Fact]] = {}
    for fact in facts:
        by_device.setdefault(fact.device, []).append(fact)

    os.makedirs(out_dir, exist_ok=True)
    rows: list[dict[str, Any]] = []
    devices_doc: list[dict[str, Any]] = []
    for device in sorted(by_device):
        programs, n_candidates = settle_device(by_device[device], contract)
        devices_doc.append({
            "device": device,
            "facts": len(by_device[device]),
            "candidate_programs": n_candidates,
            "selected_programs": len(programs),
            "total_span": sum(p.span for p in programs),
            "programs": [_program_to_dict(p) for p in programs],
        })
        for pi, program in enumerate(programs):
            for si, fact in enumerate(program.facts):
                rows.append({
                    "device": device,
                    "program_index": pi,
                    "step_index": si,
                    "marker": fact.marker,
                    "ts": fact.ts,
                    "sample_id": fact.sample_id,
                    "hit_indices": list(fact.hit_indices),
                    "program_span": program.span,
                    "program_start_ts": program.start_ts,
                    "program_end_ts": program.end_ts,
                    PROVENANCE_FILE: fact.src_file,
                    PROVENANCE_RG: fact.src_rg,
                    PROVENANCE_ROW: fact.src_row,
                })

    schema = settlement_schema()
    table = pa.table(
        {f.name: pa.array([r[f.name] for r in rows], type=f.type) for f in schema},
        schema=schema,
    )
    parquet_path = os.path.join(out_dir, f"{batch_id}.parquet")
    pq.write_table(table, parquet_path, write_statistics=True)

    doc = {
        "batch_id": batch_id,
        "contract": {"steps": list(contract.steps), "max_gaps": list(contract.max_gaps)},
        "rule": "每台设备挑足最多互不重叠程序（闭区间）；数量并列先取总跨度较小，"
                "再按各段依次使用的采样号序列字典序定胜负；"
                "步与步必须来自同一设备上严格递增时刻的不同采样，相邻步时间差不超过合同上限",
        "limits": {"max_steps": MAX_STEPS, "max_samples_per_device": MAX_SAMPLES_PER_DEVICE},
        "marker_files": paths,
        "devices": devices_doc,
        "output_parquet": parquet_path,
    }
    json_path = os.path.join(out_dir, f"{batch_id}.json")
    with open(json_path, "w", encoding="utf-8") as fh:
        json.dump(doc, fh, ensure_ascii=False, indent=2)
    return doc
