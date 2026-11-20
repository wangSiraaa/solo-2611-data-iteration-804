"""命令行入口。

用法：
    python -m pqscan.cli run --files fixtures/files.json --queries fixtures/queries.json \
        --out output
    python -m pqscan.cli verify --files ... --queries ... --out output   # 只跑独立核对
    python -m pqscan.cli settle --batch fixtures/bench/batch-ab.json \
        [--markers output/results/m_a.parquet ...] [--out output]        # 台架程序批处理结算
"""
from __future__ import annotations

import argparse
import json
import sys

from .errors import PqScanError
from .expr import validate, validate_projection
from .nested import (
    MarkerQuery,
    parse_query_item,
    reference_marker_scan,
    validate_marker,
)
from .reference import reference_scan
from .runner import run
from .settle import run_settlement


def _cmd_run(args: argparse.Namespace) -> int:
    doc = run(args.files, args.queries, args.out)
    all_ok = all(q["passed"] for q in doc["verification"]["queries"])
    for q in doc["verification"]["queries"]:
        flag = "PASS" if q["passed"] else "FAIL"
        print(f"[verify:{flag}] {q['query_id']}: {q['detail']} "
              f"(engine={q['engine_rows']}, reference={q['reference_rows']})")
    print(f"\n计划与结果已写入 {args.out}/ (plans/、results/、run_record.json、verification.json)")
    return 0 if all_ok else 2


def _cmd_verify(args: argparse.Namespace) -> int:
    import os
    import pyarrow.parquet as pq

    fixture_dir = os.path.dirname(os.path.abspath(args.files))
    with open(args.files, encoding="utf-8") as fh:
        files = [os.path.join(fixture_dir, n) for n in json.load(fh)]
    with open(args.queries, encoding="utf-8") as fh:
        queries = [parse_query_item(item) for item in json.load(fh)]

    failed = 0
    for query in queries:
        for path in files:
            schema = pq.ParquetFile(path).schema_arrow
            if isinstance(query, MarkerQuery):
                validate_marker(query, schema)
            else:
                validate(query.where, schema)
                validate_projection(query.columns, schema)
        if isinstance(query, MarkerQuery):
            ref = reference_marker_scan(query, files)
        else:
            ref = reference_scan(query, files)
        result_path = os.path.join(args.out, "results", f"{query.id}.parquet")
        got = pq.read_table(result_path)
        same = got.schema == ref.schema and got.to_pylist() == ref.to_pylist()
        print(f"[{'PASS' if same else 'FAIL'}] {query.id}: {got.num_rows} 行")
        failed += 0 if same else 1
    return 0 if failed == 0 else 2


def _cmd_settle(args: argparse.Namespace) -> int:
    import os

    out_dir = os.path.join(args.out, "settlement")
    doc = run_settlement(args.batch, args.markers, out_dir)
    total = 0
    for dev in doc["devices"]:
        total += dev["selected_programs"]
        print(f"[settle] 设备 {dev['device']}: 候选 {dev['candidate_programs']} 段，"
              f"选中 {dev['selected_programs']} 段，总跨度 {dev['total_span']}")
    print(f"\n批 {doc['batch_id']} 共结算 {total} 段；结果已写入 {out_dir}/ "
          f"({doc['batch_id']}.parquet、{doc['batch_id']}.json)")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="pqscan", description="Parquet footer 统计驱动的保守 plan/scan")
    sub = parser.add_subparsers(dest="command", required=True)

    p_run = sub.add_parser("run", help="计划 + 扫描 + 独立核对（平面查询与标记查询）")
    p_run.add_argument("--files", required=True)
    p_run.add_argument("--queries", required=True)
    p_run.add_argument("--out", default="output")
    p_run.set_defaults(func=_cmd_run)

    p_ver = sub.add_parser("verify", help="对已产出的结果再跑一次无剪枝全扫描核对")
    p_ver.add_argument("--files", required=True)
    p_ver.add_argument("--queries", required=True)
    p_ver.add_argument("--out", default="output")
    p_ver.set_defaults(func=_cmd_verify)

    p_set = sub.add_parser("settle", help="台架程序批处理结算（读取标记事实 + 程序合同）")
    p_set.add_argument("--batch", required=True, help="批描述 JSON：id/contract/marker_files")
    p_set.add_argument("--markers", nargs="*", default=None,
                       help="标记事实 Parquet 路径列表（覆盖批描述中的 marker_files）")
    p_set.add_argument("--out", default="output")
    p_set.set_defaults(func=_cmd_settle)

    args = parser.parse_args(argv)
    try:
        return args.func(args)
    except PqScanError as exc:
        print(f"错误: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
