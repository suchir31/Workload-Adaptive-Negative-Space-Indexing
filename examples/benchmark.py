"""Small repeatable migration benchmark; not a research performance claim."""
import argparse
from collections import Counter
from dataclasses import asdict
import json
import math
from pathlib import Path
from statistics import median
from tempfile import TemporaryDirectory
from time import perf_counter

import pyarrow as pa
import pyarrow.parquet as pq

from certskip import Interval as I, Region as R, Workload, build_index, execute, execute_sql


def rows(table):
    return Counter(tuple(row.values()) for row in table.to_pylist())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--budget", type=int, default=10000)
    args = parser.parse_args()
    if args.repeats < 5:
        parser.error("Use at least five repetitions")
    with TemporaryDirectory() as directory:
        path = Path(directory) / "correlated.parquet"
        data = []
        for group in range(20):
            block = [{"x": x, "y": x} for x in range(50)]
            if group % 5 == 0:
                block[0] = block[1] = {"x": 15, "y": 35}
            data.extend(block)
        pq.write_table(pa.Table.from_pylist(data), path, row_group_size=50)
        training = R.of(x=I.eq(10), y=I.eq(40))
        shifted = R.of(x=I.eq(15), y=I.eq(35))
        workload = Workload()
        workload.observe(training, 100)
        start = perf_counter()
        fixed = build_index([path], workload, args.budget)
        build_seconds = perf_counter() - start
        workload.observe(shifted, 1000)
        start = perf_counter()
        adapted = build_index([path], workload, args.budget)
        refresh_seconds = perf_counter() - start
        expected = Counter(tuple(row.values()) for row in data if shifted.matches(row))
        report = {"repeats": args.repeats, "budget_bytes": args.budget,
                  "build_seconds": build_seconds, "refresh_seconds": refresh_seconds,
                  "fixed_index_bytes": fixed.memory_bytes, "adapted_index_bytes": adapted.memory_bytes,
                  "matching_rows_including_duplicates": sum(expected.values()), "variants": {}}
        for name, index in [("B0_ordinary", None), ("B1_fixed", fixed), ("B3_refreshed", adapted),
                            ("native_duckdb", None)]:
            samples = []
            for _ in range(args.repeats):
                start = perf_counter()
                if name == "native_duckdb":
                    table = execute_sql([path], "SELECT * FROM certskip_input WHERE " + shifted.sql())
                    stats = None
                else:
                    result = execute([path], shifted, index)
                    table, stats = result.table, asdict(result.stats)
                samples.append(perf_counter() - start)
                assert rows(table) == expected, name + " returned different rows"
            report["variants"][name] = {
                "p50_seconds": median(samples),
                "p95_seconds": sorted(samples)[math.ceil(.95 * len(samples)) - 1],
                "stats": stats,
            }
        print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
