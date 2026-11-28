"""Command-line workflows for local immutable Parquet files."""
import argparse
from dataclasses import asdict
import json
from pathlib import Path
import sys

import pyarrow.parquet as pq

from .engine import build_index_report, execute, execute_sql
from .experiments import benchmark, example_workloads, generate_dataset
from .predicates import Region
from .storage import _read_json, _write_json, load_index, load_workload, save_index, save_workload
from .engine import Workload


def main(argv=None):
    parser = argparse.ArgumentParser(prog="certskip", description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    generate = commands.add_parser("generate", help="Generate reproducible data and example workloads")
    generate.add_argument("directory", type=Path)
    generate.add_argument("--mode", choices=("correlated", "independent"), default="correlated")
    generate.add_argument("--groups", type=int, default=20)
    generate.add_argument("--group-size", type=int, default=100)
    generate.add_argument("--seed", type=int, default=71)
    build = commands.add_parser("build", help="Verify and save a workload-selected index")
    build.add_argument("files", nargs="+")
    build.add_argument("--workload", required=True)
    build.add_argument("--output", required=True)
    build.add_argument("--budget", type=int, default=10 * 1024 * 1024)
    build.add_argument("--candidate-limit", type=int, default=1000)
    build.add_argument("--candidate-strategy", choices=("queries", "merged"), default="queries")
    build.add_argument("--policy", choices=("overlap", "frequency"), default="overlap")
    build.add_argument("--batch-size", type=int, default=65536)
    query = commands.add_parser("query", help="Execute a typed Region or ordinary SQL")
    query.add_argument("files", nargs="+")
    predicates = query.add_mutually_exclusive_group(required=True)
    predicates.add_argument("--region", help="JSON file containing Region.to_dict()")
    predicates.add_argument("--sql", help="SELECT using the certskip_input table")
    query.add_argument("--index", help="Saved index; reverified before use")
    query.add_argument("--budget", type=int, default=10 * 1024 * 1024)
    query.add_argument("--output", help="Write result as Parquet instead of JSON rows")
    inspect = commands.add_parser("inspect", help="Inspect Parquet schemas and row-group sizes")
    inspect.add_argument("files", nargs="+")
    bench = commands.add_parser("benchmark", help="Run B0–B3 and native DuckDB experiments")
    bench.add_argument("--repeats", type=int, default=5)
    bench.add_argument("--budgets", type=int, nargs="+", default=[2048, 8192])
    bench.add_argument("--groups", type=int, default=20)
    bench.add_argument("--group-size", type=int, default=100)
    bench.add_argument("--seed", type=int, default=71)
    bench.add_argument("--output", required=True)
    bench.add_argument("--plot")
    args = parser.parse_args(argv)
    try:
        if args.command == "generate":
            args.directory.mkdir(parents=True, exist_ok=True)
            path = args.directory / "data.parquet"
            generate_dataset(path, mode=args.mode, groups=args.groups, group_size=args.group_size, seed=args.seed)
            for name, queries in zip(("training", "shifted"), example_workloads()):
                workload = Workload()
                for q in queries:
                    workload.observe(q, 100)
                save_workload(workload, args.directory / (name + ".json"))
                _write_json(args.directory / (name + "-query.json"), queries[0].to_dict())
            print(json.dumps({"data": str(path), "directory": str(args.directory)}))
        elif args.command == "build":
            report = build_index_report(args.files, load_workload(args.workload), args.budget,
                                        args.candidate_limit, policy=args.policy,
                                        candidate_strategy=args.candidate_strategy, batch_size=args.batch_size)
            save_index(report.index, args.output)
            print(json.dumps(asdict(report.stats)))
        elif args.command == "query":
            if args.sql is not None:
                if args.index:
                    raise ValueError("SQL fallback does not use an index; omit --index")
                table, stats = execute_sql(args.files, args.sql), None
            else:
                index = load_index(args.index, args.files, args.budget) if args.index else None
                result = execute(args.files, Region.from_dict(_read_json(args.region)), index)
                table, stats = result.table, asdict(result.stats)
            if args.output:
                pq.write_table(table, args.output)
                print(json.dumps({"output": args.output, "rows": table.num_rows, "stats": stats}))
            else:
                print(json.dumps({"rows": table.to_pylist(), "stats": stats}, default=str))
        elif args.command == "inspect":
            output = []
            for path in args.files:
                with pq.ParquetFile(path) as parquet:
                    output.append({"path": str(Path(path).resolve()), "schema": str(parquet.schema_arrow),
                                   "rows": parquet.metadata.num_rows, "row_groups": parquet.num_row_groups})
            print(json.dumps(output))
        elif args.command == "benchmark":
            report = benchmark(repeats=args.repeats, budgets=args.budgets, groups=args.groups,
                               group_size=args.group_size, seed=args.seed, output=args.output, plot=args.plot)
            print(json.dumps({"output": args.output, "comparisons": len(report["results"])}))
    except (ValueError, TypeError, KeyError, OSError, RuntimeError) as error:
        parser.exit(2, "certskip: " + str(error) + "\n")
    return 0
