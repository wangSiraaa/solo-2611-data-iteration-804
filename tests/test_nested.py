"""pytest 覆盖：嵌套探头读数标记查询（3VL 语义、footer 计划、两阶段扫描、独立核对）
与台架程序批处理结算（最多段数、总跨度、采样号字典序、闭区间、上限校验）。

运行：PYTHONPATH=src .venv/bin/python -m pytest tests/ -q
"""
from __future__ import annotations

import json
import os

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pqscan.errors import PlanError
from pqscan.nested import (
    Cond,
    eval_exists,
    eval_marker,
    exists_arrow,
    parse_marker,
    parse_query_item,
    plan_marker_file,
    read_nested_footer_stats,
    validate_marker,
)
from pqscan.runner import run
from pqscan.settle import (
    Contract,
    Fact,
    enumerate_programs,
    load_facts,
    run_settlement,
    select_programs,
    settle_device,
    validate_contract,
)

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BENCH_DIR = os.path.join(REPO, "fixtures", "bench")
BENCH_FILES = [os.path.join(BENCH_DIR, "bench-a.parquet"), os.path.join(BENCH_DIR, "bench-b.parquet")]

P_GE8 = (Cond("eq", "probe", "P"), Cond("ge", "value", 8))


def _marker(**kw) -> object:
    item = {"id": "m", "marker": "M", "exists": [{"eq": ["probe", "P"]}, {"ge": ["value", 8]}]}
    item.update(kw)
    return parse_marker(item)


# ---------------------------------------------------------------------------
# 三值逻辑语义（纯 Python 路径）
# ---------------------------------------------------------------------------

def test_exists_three_valued_semantics():
    assert eval_exists(P_GE8, None) == (None, [])                 # 列表 NULL -> 未知
    assert eval_exists(P_GE8, []) == (False, [])                  # 空列表 -> 假
    assert eval_exists(P_GE8, [None]) == (None, [])               # 只有未知元素 -> 未知
    # 需求示例：[NULL, ('P',8)] 命中下标 1
    assert eval_exists(P_GE8, [None, {"probe": "P", "value": 8}]) == (True, [1])
    # 需求示例：[('P',2),('Q',9)] 不能满足“探头 P 且值>=8”（不跨元素拼值）
    assert eval_exists(P_GE8, [{"probe": "P", "value": 2},
                               {"probe": "Q", "value": 9}]) == (False, [])
    # 全部真元素下标都要列出
    assert eval_exists(P_GE8, [{"probe": "P", "value": 8},
                               {"probe": "P", "value": 9}]) == (True, [0, 1])
    # 字段为 NULL 的元素是未知元素
    assert eval_exists(P_GE8, [{"probe": "P", "value": None}]) == (None, [])
    # 无真元素但有未知元素 -> 未知（即使其余元素确定为假）
    assert eval_exists(P_GE8, [{"probe": "Q", "value": 9},
                               {"probe": "P", "value": None}]) == (None, [])


def test_negated_marker_only_outputs_true_with_empty_evidence():
    q = _marker(negate=True)
    # 存在条件确定为假 -> 取反命中，证据下标为空
    assert eval_marker(q, [{"probe": "P", "value": 2}]) == (True, [])
    assert eval_marker(q, []) == (True, [])
    # 存在条件为真 -> 不命中
    assert eval_marker(q, [{"probe": "P", "value": 8}]) == (False, [])
    # 未知（NULL 列表 / 含未知元素）取反仍未知 -> 不命中
    assert eval_marker(q, None) == (False, [])
    assert eval_marker(q, [None]) == (False, [])
    # 正存在条件命中时证据为全部真元素下标
    q_pos = _marker()
    assert eval_marker(q_pos, [None, {"probe": "P", "value": 8},
                               {"probe": "P", "value": 9}]) == (True, [1, 2])


def test_arrow_engine_matches_python_rows():
    """PyArrow 内核路径与纯 Python 路径逐行一致（覆盖 NULL/空表/未知元素）。"""
    readings_col = pa.array(
        [
            None,
            [],
            [None],
            [None, {"probe": "P", "value": 8}],
            [{"probe": "P", "value": 2}, {"probe": "Q", "value": 9}],
            [{"probe": "P", "value": None}],
            [{"probe": "P", "value": 8}, {"probe": "P", "value": 9}],
            [{"probe": "Q", "value": 1}, None],
        ],
        type=pa.list_(pa.field("element", pa.struct([
            ("probe", pa.string()), ("value", pa.int64())
        ]))),
    )
    truths, evidence = exists_arrow(readings_col, P_GE8)
    expected = [eval_exists(P_GE8, row) for row in readings_col.to_pylist()]
    assert list(zip(truths, evidence)) == expected


def test_arrow_engine_masks_garbage_under_null_elements():
    """NULL 元素的子字段缓冲区可能残留任意取值（如 ('P',100)），
    引擎必须按“元素为空则未知”处理，不能据此拼出命中。"""
    probe = pa.array(["P", "P"])
    value = pa.array([100, 100])
    elem_type = pa.struct([("probe", pa.string()), ("value", pa.int64())])
    # 两个元素都是 NULL struct，但子字段缓冲区是 ('P',100)——足以“假命中”
    structs = pa.StructArray.from_arrays(
        [probe, value], fields=elem_type, mask=pa.array([True, True])
    )
    readings = pa.ListArray.from_arrays(pa.array([0, 2]), structs)
    assert readings.to_pylist() == [[None, None]]
    truths, evidence = exists_arrow(readings, P_GE8)
    assert truths == [None] and evidence == [[]]
    assert eval_exists(P_GE8, [None, None]) == (None, [])


def test_arrow_engine_fuzz_parity_with_python():
    """确定性伪随机数据上，两条代码路径逐行一致。"""
    import random

    rng = random.Random(20261120)
    probes = ["P", "Q", "R", None]
    rows = []
    for _ in range(500):
        kind = rng.random()
        if kind < 0.1:
            rows.append(None)
            continue
        n = rng.randrange(0, 5)
        lst = []
        for _ in range(n):
            if rng.random() < 0.2:
                lst.append(None)
            else:
                lst.append({
                    "probe": rng.choice(probes),
                    "value": None if rng.random() < 0.2 else rng.randrange(0, 12),
                })
        rows.append(lst)
    col = pa.array(rows, type=pa.list_(pa.field("element", pa.struct([
        ("probe", pa.string()), ("value", pa.int64())
    ]))))
    conds = (Cond("eq", "probe", "P"), Cond("ge", "value", 8))
    truths, evidence = exists_arrow(col, conds)
    expected = [eval_exists(conds, row) for row in col.to_pylist()]
    assert list(zip(truths, evidence)) == expected


# ---------------------------------------------------------------------------
# 查询解析与校验
# ---------------------------------------------------------------------------

def test_marker_parse_and_validate_errors():
    schema = pq.ParquetFile(BENCH_FILES[0]).schema_arrow
    validate_marker(_marker(), schema)  # 合法查询不报错
    validate_marker(_marker(negate=True), schema)

    with pytest.raises(PlanError):
        parse_marker({"id": "x", "marker": "M", "exists": []})            # 空条件组
    with pytest.raises(PlanError):
        parse_marker({"id": "x", "marker": "M",
                      "exists": [{"and": [{"eq": ["probe", "P"]}]}]})     # 组内不嵌套 and
    with pytest.raises(PlanError):
        parse_marker({"id": "x", "marker": "M", "exists": [{"eq": ["probe", "P"]}],
                      "negate": "yes"})                                    # negate 必须是布尔
    with pytest.raises(PlanError):
        validate_marker(_marker(exists=[{"eq": ["nope", 1]}]), schema)     # 元素无此字段
    with pytest.raises(PlanError):
        validate_marker(_marker(exists=[{"eq": ["value", "P"]}]), schema)  # 字面量类型不符
    with pytest.raises(PlanError):
        validate_marker(_marker(exists=[{"eq": ["probe", True]}]), schema)
    # 平面文件（无读数列）上的标记查询要在计划期报错
    flat_schema = pa.schema([("k", pa.int64())])
    with pytest.raises(PlanError):
        validate_marker(_marker(), flat_schema)


def test_parse_query_item_dispatches_by_shape():
    assert parse_query_item({"id": "m", "marker": "M",
                             "exists": [{"eq": ["probe", "P"]}]}).marker == "M"
    flat = parse_query_item({"id": "f", "where": {"eq": ["a", 1]}, "columns": ["a"]})
    assert flat.id == "f" and flat.columns == ("a",)


# ---------------------------------------------------------------------------
# 计划阶段：只用单列统计证伪，绝不拼跨列极值
# ---------------------------------------------------------------------------

def _plans(query):
    return {
        path: plan_marker_file(path, query, read_nested_footer_stats(
            path, pq.ParquetFile(path).schema_arrow, query))
        for path in BENCH_FILES
    }


def test_marker_planner_skips_only_via_single_column_disproof():
    # A = (probe='P' AND value>=8)
    a = _marker(id="m_a", marker="A")
    plans = _plans(a)
    act = {p: [d.action for d in ds] for p, ds in plans.items()}
    # bench-a RG1 的 probe 全为 'R'（'P' 不在 [min,max] 内）-> skip
    # bench-b RG0 的 probe 只有 'Q'/'R' -> skip；其余组保留
    assert act[BENCH_FILES[0]] == ["keep", "skip"]
    assert act[BENCH_FILES[1]] == ["skip", "keep"]
    # 被跳过的组：trace 里 probe 条件 can_be_true=False，且只用到 probe 一列的统计
    trace = plans[BENCH_FILES[0]][1].trace
    assert trace.can_be_true is False
    leaf = next(c for c in trace.children if c.op == "eq")
    assert leaf.can_be_true is False and set(leaf.stats_used) == {"probe"}


def test_marker_planner_value_column_disproof():
    # C = (probe='R' AND value>=100)：bench-a RG0 的 probe 不含 'R'，
    # RG1 的 value 最大为 5 -> 两组都可跳过，但证伪依据不同列
    c = _marker(id="m_c", marker="C",
                exists=[{"eq": ["probe", "R"]}, {"ge": ["value", 100]}])
    plans = _plans(c)
    assert [d.action for d in plans[BENCH_FILES[0]]] == ["skip", "skip"]
    rg0, rg1 = plans[BENCH_FILES[0]]
    assert rg0.trace.children[0].can_be_true is False   # probe 证伪
    assert rg1.trace.children[1].can_be_true is False   # value 证伪


def test_negated_marker_keeps_all_groups():
    n = _marker(id="m_n", marker="N", negate=True,
                exists=[{"eq": ["probe", "R"]}, {"ge": ["value", 100]}])
    for decisions in _plans(n).values():
        assert all(d.action == "keep" for d in decisions)


def test_marker_planner_conservative_without_stats(tmp_path):
    """无统计文件上的标记查询必须全部保守保留。"""
    import shutil

    dst = str(tmp_path / "nostats.parquet")
    shutil.copy(BENCH_FILES[0], dst)
    # 重写为无统计版本
    tbl = pq.read_table(dst)
    with pq.ParquetWriter(dst, tbl.schema, write_statistics=False) as w:
        w.write_table(tbl)
    q = _marker()
    decisions = plan_marker_file(dst, q, read_nested_footer_stats(dst, tbl.schema, q))
    assert all(d.action == "keep" for d in decisions)
    assert all(d.missing_stats for d in decisions)


# ---------------------------------------------------------------------------
# 端到端：标记查询 run + 独立核对 + 读取记录
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def bench_run(tmp_path_factory):
    out = str(tmp_path_factory.mktemp("bench_out"))
    doc = run(os.path.join(BENCH_DIR, "files.json"),
              os.path.join(BENCH_DIR, "queries.json"), out)
    return out, doc


def test_marker_end_to_end_verification_and_facts(bench_run):
    out, doc = bench_run
    assert all(q["passed"] for q in doc["verification"]["queries"])

    def facts(qid):
        return [(
            r["marker"], r["device"], r["ts"], r["sample_id"], r["hit_indices"],
            os.path.basename(r["_source_file"]), r["_source_row_group"], r["_source_row_in_group"],
        ) for r in pq.read_table(os.path.join(out, "results", f"{qid}.parquet")).to_pylist()]

    assert facts("m_a") == [
        ("A", "D1", 0, 100, [0], "bench-a.parquet", 0, 0),
        ("A", "D1", 2, 101, [0], "bench-a.parquet", 0, 1),
        ("A", "D3", 7, 301, [0], "bench-a.parquet", 0, 3),
        ("A", "D1", 4, 103, [1], "bench-b.parquet", 1, 0),   # [NULL,('P',8)] 命中下标 1
    ]
    assert facts("m_b") == [
        ("B", "D3", 7, 301, [1], "bench-a.parquet", 0, 3),
        ("B", "D1", 3, 102, [0], "bench-b.parquet", 0, 0),
        ("B", "D1", 5, 104, [0], "bench-b.parquet", 1, 1),
    ]
    assert facts("m_c") == [
        ("C", "D2", 12, 203, [0], "bench-b.parquet", 1, 2),
        ("C", "D2", 12, 206, [0], "bench-b.parquet", 1, 3),  # 同刻多个采样
    ]
    n_facts = facts("m_n")
    # 取反而命中：证据下标一律为空
    assert all(idx == [] for *_, idx, _f, _g, _r in
               [(r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7]) for r in n_facts])
    n_ids = {(d, ts, sid) for _m, d, ts, sid, _i, *_ in n_facts}
    assert ("D2", 10, 200) in n_ids          # 存在条件确定为假 -> 取反命中
    assert ("D3", 9, 302) in n_ids           # 空列表 -> 假 -> 取反命中
    # 未知不取反为真：NULL 列表 / NULL 元素 / NULL 字段 / 含未知元素的列表都不命中
    assert ("D2", 10, 201) not in n_ids      # 列表 NULL
    assert ("D2", 11, 202) not in n_ids      # [NULL]
    assert ("D2", 14, 205) not in n_ids      # [('R', NULL)]
    assert ("D1", 4, 103) not in n_ids       # [NULL, ('P',8)] 存在未知元素
    # 存在条件为真的行不命中取反查询
    assert ("D2", 12, 203) not in n_ids and ("D2", 12, 206) not in n_ids


def test_same_sample_two_markers_vs_two_samples(bench_run):
    """同一采样命中两种标记（id301 -> A、B 各一行）与两次不同采样（A@0、A@2）可区分。"""
    out, _ = bench_run
    a = pq.read_table(os.path.join(out, "results", "m_a.parquet")).to_pylist()
    b = pq.read_table(os.path.join(out, "results", "m_b.parquet")).to_pylist()
    same_sample = {r["sample_id"] for r in a} & {r["sample_id"] for r in b}
    assert same_sample == {301}
    a_ids = [r["sample_id"] for r in a if r["device"] == "D1"]
    assert a_ids == [100, 101, 103]          # 同一标记的三次不同采样


def test_marker_run_record_two_phase_reads(bench_run):
    out, _ = bench_run
    with open(os.path.join(out, "run_record.json"), encoding="utf-8") as fh:
        record = json.load(fh)
    per_query = {q["query_id"]: q for q in record["queries"]}
    # m_a：bench-a RG1 与 bench-b RG0 计划跳过，零读取
    skipped = [g for g in per_query["m_a"]["groups"] if g["status"] == "planned_skip"]
    assert len(skipped) == 2 and all(g["column_reads"] == [] for g in skipped)
    # 命中组：filter 阶段只读读数列，projection 阶段只读设备/时刻/采样号
    for q in per_query.values():
        for g in q["groups"]:
            if g["status"] == "matched":
                phases = [(r["phase"], tuple(r["columns"])) for r in g["column_reads"]]
                assert ("filter", ("readings",)) in phases
                assert all(set(cols) <= {"device", "ts", "sample_id"}
                           for phase, cols in phases if phase == "projection")


def test_marker_empty_result_schema(bench_run):
    out, _ = bench_run
    queries = [{"id": "m_z", "marker": "Z", "exists": [{"eq": ["probe", "ZZ"]}]}]
    qjson = os.path.join(out, "queries_z.json")
    with open(qjson, "w", encoding="utf-8") as fh:
        json.dump(queries, fh)
    doc = run(os.path.join(BENCH_DIR, "files.json"), qjson, out)
    assert doc["verification"]["queries"][0]["passed"]
    empty = pq.read_table(os.path.join(out, "results", "m_z.parquet"))
    assert empty.num_rows == 0
    assert empty.schema.names == ["marker", "device", "ts", "sample_id", "hit_indices",
                                  "_source_file", "_source_row_group", "_source_row_in_group"]
    assert pa.types.is_list(empty.schema.field("hit_indices").type)


# ---------------------------------------------------------------------------
# 批处理结算
# ---------------------------------------------------------------------------

def mkfact(device: str, ts: int, sample_id: int, marker: str) -> Fact:
    return Fact(device=device, ts=ts, sample_id=sample_id, marker=marker,
                hit_indices=(), src_file="f", src_rg=0, src_row=0)


def test_settle_prompt_example():
    """需求示例：A@0、A@2、B@3、B@5（不同采样）-> 只选 2->3（一段、跨度 1）；
    增加 A@4 后可选 2->3 与 4->5；间隔上限 0 则一段也不能结算。"""
    base = [mkfact("D", 0, 1, "A"), mkfact("D", 2, 2, "A"),
            mkfact("D", 3, 3, "B"), mkfact("D", 5, 4, "B")]
    contract = Contract(steps=("A", "B"), max_gaps=(3,))

    programs, n_cand = settle_device(base, contract)
    assert n_cand == 3                        # 0->3、2->3、2->5（0->5 间隔 5>3 排除）
    assert [[f.sample_id for f in p.facts] for p in programs] == [[2, 3]]
    assert programs[0].span == 1

    with_a4 = base + [mkfact("D", 4, 5, "A")]
    programs, n_cand = settle_device(with_a4, contract)
    assert n_cand == 4
    assert [[f.sample_id for f in p.facts] for p in programs] == [[2, 3], [5, 4]]
    assert [p.span for p in programs] == [1, 1]

    gap0 = Contract(steps=("A", "B"), max_gaps=(0,))
    programs, n_cand = settle_device(with_a4, gap0)
    assert n_cand == 0 and programs == []     # 严格递增时刻下间隔不可能 <=0


def test_settle_gap_boundary_inclusive():
    facts = [mkfact("D", 0, 1, "A"), mkfact("D", 3, 2, "B")]
    ok, _ = settle_device(facts, Contract(("A", "B"), (3,)))    # 边界值可用
    assert len(ok) == 1 and ok[0].span == 3
    no, n_cand = settle_device(facts, Contract(("A", "B"), (2,)))
    assert no == [] and n_cand == 0


def test_settle_closed_intervals_cannot_touch():
    """闭区间：前段末时刻 == 后段首时刻也算重叠，不能同时结算。"""
    facts = [mkfact("D", 2, 1, "A"), mkfact("D", 3, 2, "B"),
             mkfact("D", 3, 3, "A"), mkfact("D", 5, 4, "B")]
    candidates = enumerate_programs(facts, Contract(("A", "B"), (3,)))
    spans = {(p.start_ts, p.end_ts) for p in candidates}
    assert spans == {(2, 3), (2, 5), (3, 5)}
    programs, _ = settle_device(facts, Contract(("A", "B"), (3,)))
    # 若允许端点相接，[2,3]+[3,5] 可达两段；闭区间下只能一段，取跨度最小的 [2,3]
    assert len(programs) == 1
    assert (programs[0].start_ts, programs[0].end_ts) == (2, 3)


def test_settle_total_span_then_sample_seq_tiebreak():
    """数量并列先比总跨度，再比各段依次使用的采样号序列字典序。"""
    # 两种两段方案：[0,1]+[4,5]（跨度 2）与 [0,5]（一段）-> 段数多者胜
    facts = [mkfact("D", 0, 1, "A"), mkfact("D", 1, 2, "B"),
             mkfact("D", 4, 3, "A"), mkfact("D", 5, 4, "B")]
    programs, _ = settle_device(facts, Contract(("A", "B"), (9,)))
    assert [[f.sample_id for f in p.facts] for p in programs] == [[1, 2], [3, 4]]

    # 同刻两个采样命中同一标记：候选段跨度相同，采样号字典序小者胜
    facts = [mkfact("D", 12, 203, "C"), mkfact("D", 12, 206, "C"),
             mkfact("D", 13, 204, "N")]
    candidates = enumerate_programs(facts, Contract(("C", "N"), (2,)))
    assert len(candidates) == 2
    programs = select_programs(candidates)
    assert [f.sample_id for f in programs[0].facts] == [203, 204]


def test_settle_same_ts_cannot_chain():
    """同刻多个采样不能相接（时刻必须严格递增）；同一采样不能充当两步。"""
    facts = [mkfact("D", 7, 301, "A"), mkfact("D", 7, 301, "B")]
    programs, n_cand = settle_device(facts, Contract(("A", "B"), (9,)))
    assert programs == [] and n_cand == 0
    # 同刻但不同采样：依然不能相接
    facts = [mkfact("D", 7, 301, "A"), mkfact("D", 7, 302, "B")]
    programs, n_cand = settle_device(facts, Contract(("A", "B"), (9,)))
    assert programs == [] and n_cand == 0


def test_settle_limits_and_input_validation(tmp_path):
    with pytest.raises(PlanError):
        validate_contract({"steps": ["A", "B", "C", "D", "E"], "max_gaps": [1, 1, 1, 1]})
    with pytest.raises(PlanError):
        validate_contract({"steps": ["A", "B"], "max_gaps": [1, 2]})
    with pytest.raises(PlanError):
        validate_contract({"steps": ["A", "B"], "max_gaps": [-1]})
    with pytest.raises(PlanError):
        validate_contract({"steps": [], "max_gaps": []})
    # 每台设备最多 60 个采样
    facts = [mkfact("D", i, i, "A") for i in range(61)]
    with pytest.raises(PlanError):
        settle_device(facts, Contract(("A",), ()))
    programs, _ = settle_device(facts[:60], Contract(("A",), ()))
    assert len(programs) == 60                # 单步程序、跨度 0、互不重叠


def test_settle_conflicting_sample_ts(tmp_path):
    """同一 (设备, 采样号) 出现两个时刻 -> 矛盾输入，报错。"""
    import pyarrow as pa
    from pqscan.nested import fact_schema

    tbl = pa.table({
        "marker": ["A", "A"],
        "device": ["D", "D"],
        "ts": [1, 2],
        "sample_id": [7, 7],
        "hit_indices": [[0], [0]],
        "_source_file": ["f", "f"],
        "_source_row_group": [0, 0],
        "_source_row_in_group": [0, 1],
    }, schema=fact_schema(pa.string(), pa.int64(), pa.int64()))
    path = str(tmp_path / "facts.parquet")
    pq.write_table(tbl, path)
    with pytest.raises(PlanError):
        load_facts([path])


def test_settle_batch_end_to_end(bench_run, tmp_path):
    """完整流水线：标记查询产物 -> 三份合同批处理结算。"""
    out, _ = bench_run
    results = os.path.join(out, "results")

    def settle(batch):
        return run_settlement(
            os.path.join(BENCH_DIR, batch),
            [os.path.join(results, f"{m}.parquet") for m in ("m_a", "m_b", "m_c", "m_n")],
            str(tmp_path),
        )

    doc = settle("batch-ab.json")
    d1 = next(d for d in doc["devices"] if d["device"] == "D1")
    assert d1["selected_programs"] == 2 and d1["total_span"] == 2
    seq = [[s["sample_id"] for s in p["steps"]] for p in d1["programs"]]
    assert seq == [[101, 102], [103, 104]]
    # 跨文件成段：第一段的两步分别来自 bench-a 与 bench-b
    files = {os.path.basename(s["source"]["file"]) for s in d1["programs"][0]["steps"]}
    assert files == {"bench-a.parquet", "bench-b.parquet"}
    # 证据下标随步带出（A@4 来自 [NULL,('P',8')]，下标为 1）
    assert d1["programs"][1]["steps"][0]["hit_indices"] == [1]

    doc = settle("batch-cn.json")
    d2 = next(d for d in doc["devices"] if d["device"] == "D2")
    assert d2["selected_programs"] == 1
    steps = d2["programs"][0]["steps"]
    assert [s["sample_id"] for s in steps] == [203, 204]   # 同刻两个 C，字典序取 203
    assert steps[1]["hit_indices"] == []                   # 取反而命中，证据为空

    doc = settle("batch-ab-gap0.json")
    assert all(d["selected_programs"] == 0 for d in doc["devices"])

    # 结算 Parquet：每行一步，含程序区间与溯源
    tbl = pq.read_table(os.path.join(tmp_path, "batch-ab.parquet"))
    assert tbl.schema.names[0] == "device" and "program_span" in tbl.schema.names
    rows = tbl.to_pylist()
    assert [(r["program_index"], r["step_index"], r["marker"], r["sample_id"]) for r in rows] == [
        (0, 0, "A", 101), (0, 1, "B", 102), (1, 0, "A", 103), (1, 1, "B", 104),
    ]
