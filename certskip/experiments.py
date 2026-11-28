"""Reproducible B0–B3 evaluation on correlated and independent local data."""
from collections import Counter
from dataclasses import asdict
import math
from pathlib import Path
import random
from statistics import median
from tempfile import TemporaryDirectory
from time import perf_counter

import pyarrow as pa
import pyarrow.parquet as pq

from .engine import Workload, build_index_report, execute, execute_sql
from .predicates import Interval as I, Region as R
from .storage import _write_json


def generate_dataset(path, *, mode="correlated", groups=20, group_size=100, seed=71):
    if mode not in ("correlated", "independent"):
        raise ValueError("Mode must be correlated or independent")
    if type(groups) is not int or groups <= 0 or type(group_size) is not int or group_size <= 0:
        raise ValueError("Group counts and sizes must be positive integers")
    rng = random.Random(seed)
    data = []
    for group in range(groups):
        for _ in range(group_size):
            x = rng.randrange(100)
            y = rng.randrange(100) if mode == "independent" or group % 5 == 0 else x
            data.append({"x": x, "y": y})
    table = pa.Table.from_pylist(data)
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, path, row_group_size=group_size)
    return table


def example_workloads():
    first = [R.of(x=I(20, 30), y=I(60, 70)), R.of(x=I(22, 24), y=I(62, 64))]
    shifted = [R.of(x=I(60, 70), y=I(20, 30)), R.of(x=I(62, 64), y=I(22, 24))]
    return first, shifted


def _workload(queries):
    workload = Workload()
    for q in queries:
        workload.observe(q, 100)
    return workload


def _rows(table):
    return Counter(tuple(row.values()) for row in table.to_pylist())


def benchmark(*, repeats=5, budgets=(2048, 8192), groups=20, group_size=100, seed=71,
              output=None, plot=None):
    if type(repeats) is not int or repeats < 5:
        raise ValueError("At least five repetitions are required")
    budgets = tuple(budgets)
    if not budgets:
        raise ValueError("At least one budget is required")
    first, shifted = example_workloads()
    report = {"format": "certskip-benchmark-v1", "seed": seed, "repeats": repeats,
              "groups": groups, "group_size": group_size,
              "byte_metrics": "estimated compressed column-chunk bytes; excludes footers",
              "versions": {"pyarrow": pa.__version__}, "results": []}
    import duckdb
    report["versions"]["duckdb"] = duckdb.__version__
    with TemporaryDirectory() as directory:
        for mode in ("correlated", "independent"):
            path = Path(directory) / (mode + ".parquet")
            data = generate_dataset(path, mode=mode, groups=groups, group_size=group_size, seed=seed)
            for budget in budgets:
                fixed = build_index_report([path], _workload(first), budget)
                for epoch, queries in enumerate((first, shifted)):
                    frequency = build_index_report([path], _workload(queries), budget, policy="frequency")
                    adaptive = build_index_report([path], _workload(queries), budget, policy="overlap")
                    variants = {"B0": None, "B1": fixed, "B2": frequency, "B3": adaptive, "DuckDB": None}
                    expected = {q: Counter(tuple(row.values()) for row in data.to_pylist() if q.matches(row))
                                for q in queries}
                    measurements = {name: [] for name in variants}
                    # One warmup, then interleave variants in a seeded shuffled order.
                    order_rng = random.Random(seed + epoch)
                    for repetition in range(repeats + 1):
                        order = list(variants)
                        order_rng.shuffle(order)
                        for query in queries:
                            for name in order:
                                start = perf_counter()
                                if name == "DuckDB":
                                    table = execute_sql([path], "SELECT * FROM certskip_input WHERE " + query.sql())
                                    stats = None
                                else:
                                    index = variants[name].index if variants[name] else None
                                    result = execute([path], query, index)
                                    table, stats = result.table, asdict(result.stats)
                                elapsed = perf_counter() - start
                                if _rows(table) != expected[query]:
                                    raise AssertionError(name + " returned different rows (including duplicates)")
                                if repetition:
                                    measurements[name].append((elapsed, stats))
                    for name, samples in measurements.items():
                        latencies = sorted(sample[0] for sample in samples)
                        build = variants[name]
                        stats = [sample[1] for sample in samples if sample[1] is not None]
                        report["results"].append({
                            "dataset": mode, "budget_bytes": budget, "epoch": epoch, "variant": name,
                            "sample_count": len(samples), "p50_seconds": median(latencies),
                            "p95_seconds": latencies[math.ceil(.95 * len(latencies)) - 1],
                            "index_bytes": build.index.memory_bytes if build else 0,
                            "build": asdict(build.stats) if build else None,
                            "refresh_seconds": build.stats.total_seconds if build and name in ("B2", "B3") else 0,
                            "mean_row_groups_read": sum(s["row_groups_read"] for s in stats) / len(stats) if stats else None,
                            "mean_estimated_bytes_read": sum(s["estimated_bytes_read"] for s in stats) / len(stats) if stats else None,
                            "samples": [{"seconds": elapsed, "stats": stats} for elapsed, stats in samples],
                        })
    if output:
        _write_json(output, report)
    if plot:
        plot_report(report, plot)
    return report


def plot_report(report, path):
    """Optional static plot; install certskip[plot]."""
    try:
        import matplotlib
    except ImportError as error:
        raise RuntimeError("Plotting requires the optional certskip[plot] dependencies") from error
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    fig, axes = plt.subplots(1, 2, figsize=(11, 4), constrained_layout=True)
    for ax, mode in zip(axes, ("correlated", "independent")):
        for name in ("B0", "B1", "B2", "B3"):
            values = sorted((r for r in report["results"] if r["dataset"] == mode and
                             r["variant"] == name and r["epoch"] == 1), key=lambda r: r["budget_bytes"])
            ax.plot([r["budget_bytes"] for r in values], [r["mean_row_groups_read"] for r in values],
                    marker="o", label=name)
        ax.set(title=mode.capitalize() + ": shifted workload", xlabel="Index budget (bytes)",
               ylabel="Mean row groups read per query")
        ax.legend()
        ax.grid(alpha=.2)
    fig.savefig(path, dpi=160)
    plt.close(fig)
