"""Run with: python -m examples.demo"""
from pathlib import Path
from tempfile import TemporaryDirectory

import pyarrow as pa
import pyarrow.parquet as pq

from certskip import Interval, Region, Workload, build_index, execute


def main():
    with TemporaryDirectory() as directory:
        path = Path(directory) / "sales.parquet"
        pq.write_table(pa.table({
            "country": ["USA", "USA", "India", "India", "USA", "India"],
            "product": ["Laptop", "Phone", "Phone", "Laptop", "Phone", "Phone"],
        }), path, row_group_size=3)
        query = Region.of(country=Interval.eq("India"), product=Interval.eq("Laptop"))
        workload = Workload()
        workload.observe(query, frequency=100)
        index = build_index([path], workload, memory_budget=10 * 1024 * 1024)
        baseline = execute([path], query)
        result = execute([path], query, index)
        assert result.table.equals(baseline.table)
        print("Results:", result.table.to_pylist())
        print("Certificates:", len(index), "Resident index bytes:", index.memory_bytes)
        print("Baseline:", baseline.stats)
        print("CertSkip:", result.stats)


if __name__ == "__main__":
    main()
