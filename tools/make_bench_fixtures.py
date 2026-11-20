"""生成台架记录夹具：两个真实 Parquet 文件 + 标记查询 + 三份批合同。

数据设计（与 README「台架样例」一节对应）：
- bench-a.parquet / bench-b.parquet：同一 schema 的台架记录，各两个行组；
  一条记录 = 一台设备的一次台架采样（device / ts / sample_id / readings）。
- 覆盖：跨文件成段（D1 的 A@2 在 bench-a、B@3 在 bench-b）、未知值取反
  （NULL 列表 / NULL 元素 / NULL 字段在取反查询下都不产出命中）、
  同刻多个采样（D2 的 ts=10 与 ts=12 各有两条采样）、
  空列表（id302，取反查询下为真、证据为空）、
  以及需求示例行 [('P',2),('Q',9)]（id300，不能拼出命中）与 [NULL,('P',8)]（id103，命中下标 1）。

直接运行：python tools/make_bench_fixtures.py
"""
from __future__ import annotations

import json
import os

import pyarrow as pa
import pyarrow.parquet as pq

ELEM = pa.struct([("probe", pa.string()), ("value", pa.int64())])
SCHEMA = pa.schema([
    pa.field("device", pa.string()),
    pa.field("ts", pa.int64()),
    pa.field("sample_id", pa.int64()),
    pa.field("readings", pa.list_(pa.field("element", ELEM))),
])

# (device, ts, sample_id, readings)；readings 元素为 (probe, value) 或 None。
BENCH_A_RG0 = [
    ("D1", 0, 100, [("P", 9)]),
    ("D1", 2, 101, [("P", 8), ("Q", 9)]),
    ("D3", 1, 300, [("P", 2), ("Q", 9)]),   # 需求示例：P 与 9 分属两个元素，拼不出一次命中
    ("D3", 7, 301, [("P", 9), ("Q", 0)]),   # 同一采样同时命中标记 A 与 B
]
BENCH_A_RG1 = [
    ("D2", 10, 200, [("R", 5)]),            # 取反命中，证据为空
    ("D2", 10, 201, None),                  # 同刻另一个采样：列表 NULL -> 未知 -> 取反也不命中
    ("D2", 11, 202, [None]),                # NULL 元素 -> 未知
    ("D3", 9, 302, []),                     # 空列表 -> 存在条件为假 -> 取反命中，证据为空
]
BENCH_B_RG0 = [
    ("D1", 3, 102, [("Q", 1)]),             # B@3，与 bench-a 的 A@2 跨文件成段
    ("D2", 13, 204, [("R", 7)]),            # 取反命中
    ("D2", 14, 205, [("R", None)]),         # 字段 NULL -> 未知
]
BENCH_B_RG1 = [
    ("D1", 4, 103, [None, ("P", 8)]),       # 需求示例：NULL 元素 + 命中下标 1
    ("D1", 5, 104, [("Q", 0), ("P", 2)]),   # B@5（P=2 不满足 A）
    ("D2", 12, 203, [("R", 100)]),          # C@12，同刻两个采样之一
    ("D2", 12, 206, [("R", 150)]),          # C@12，同刻另一个采样（结算时按采样号字典序落选）
]

QUERIES = [
    {"id": "m_a", "marker": "A",
     "exists": [{"eq": ["probe", "P"]}, {"ge": ["value", 8]}]},
    {"id": "m_b", "marker": "B",
     "exists": [{"eq": ["probe", "Q"]}, {"le": ["value", 1]}]},
    {"id": "m_c", "marker": "C",
     "exists": [{"eq": ["probe", "R"]}, {"ge": ["value", 100]}]},
    {"id": "m_n", "marker": "N", "negate": True,
     "exists": [{"eq": ["probe", "R"]}, {"ge": ["value", 100]}]},
]

OUT_DIR = os.path.join(os.path.dirname(__file__), "..", "fixtures", "bench")


def _table(rows: list[tuple]) -> pa.Table:
    return pa.table(
        {
            "device": [r[0] for r in rows],
            "ts": [r[1] for r in rows],
            "sample_id": [r[2] for r in rows],
            "readings": [
                None if r[3] is None else [
                    None if e is None else {"probe": e[0], "value": e[1]} for e in r[3]
                ]
                for r in rows
            ],
        },
        schema=SCHEMA,
    )


def _write(path: str, groups: list[list[tuple]]) -> None:
    with pq.ParquetWriter(path, SCHEMA, write_statistics=True) as writer:
        for group in groups:
            writer.write_table(_table(group))


def main() -> None:
    os.makedirs(OUT_DIR, exist_ok=True)
    _write(os.path.join(OUT_DIR, "bench-a.parquet"), [BENCH_A_RG0, BENCH_A_RG1])
    _write(os.path.join(OUT_DIR, "bench-b.parquet"), [BENCH_B_RG0, BENCH_B_RG1])

    def dump(name: str, obj: object) -> None:
        with open(os.path.join(OUT_DIR, name), "w", encoding="utf-8") as fh:
            json.dump(obj, fh, ensure_ascii=False, indent=2)

    dump("files.json", ["bench-a.parquet", "bench-b.parquet"])
    dump("queries.json", QUERIES)
    results = os.path.join("..", "..", "output", "results")
    dump("batch-ab.json", {
        "id": "batch-ab",
        "contract": {"steps": ["A", "B"], "max_gaps": [3]},
        "marker_files": [f"{results}/m_a.parquet", f"{results}/m_b.parquet"],
    })
    dump("batch-cn.json", {
        "id": "batch-cn",
        "contract": {"steps": ["C", "N"], "max_gaps": [2]},
        "marker_files": [f"{results}/m_c.parquet", f"{results}/m_n.parquet"],
    })
    dump("batch-ab-gap0.json", {
        "id": "batch-ab-gap0",
        "contract": {"steps": ["A", "B"], "max_gaps": [0]},
        "marker_files": [f"{results}/m_a.parquet", f"{results}/m_b.parquet"],
    })
    print("wrote fixtures/bench/: bench-a.parquet, bench-b.parquet, "
          "files.json, queries.json, batch-ab.json, batch-cn.json, batch-ab-gap0.json")


if __name__ == "__main__":
    main()
