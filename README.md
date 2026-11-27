# CertSkip — Python / C++20

Workload-adaptive, exactly verified empty-region certificates for immutable
Parquet files. C++ verifies absent regions, selects certificates under a byte
budget, checks containment, and reads selected row groups with Apache Arrow C++.
Python coordinates workloads and file versions. DuckDB executes the query over
the selected Arrow data. DataFusion is no longer a dependency.

This checkout previously contained a Python/DataFusion prototype, not Rust source.
The v0.2 migration follows the supplied C++/Python beginner guide; see
[PROJECT_PLAN.md](PROJECT_PLAN.md) for the implementation plan and scope.

## Build and run

Use Python 3.9–3.13, a C++20 compiler, and macOS 12+ or Linux. The native reader
uses POSIX file descriptors; Windows is not currently supported. PyArrow 19.0.1
is pinned for matching build/runtime Arrow ABIs. No separate Arrow installation
is necessary: its wheel supplies the headers and shared libraries.

```sh
python3 -m venv .venv
.venv/bin/python -m pip install --upgrade pip
.venv/bin/python -m pip install -e '.[test]'
.venv/bin/python -m pytest -q
.venv/bin/python -m examples.demo
```

For CMake development and standalone C++ tests:

```sh
.venv/bin/python -m pip install cmake pybind11
.venv/bin/cmake -S . -B build/cmake -DPython_EXECUTABLE="$PWD/.venv/bin/python"
.venv/bin/cmake --build build/cmake --parallel 2
.venv/bin/ctest --test-dir build/cmake --output-on-failure
```

CMake places the extension in `certskip/`. Editable installation compiles it too;
rebuild after C++ edits. CTest uses a small dependency-free native test executable;
Python integration tests use pytest. CI runs both on Linux and macOS.

## Use

```python
from certskip import Interval, Region, Workload, build_index, execute

files = ["sales.parquet"]
query = Region.of(
    country=Interval.eq("India"),
    product=Interval.eq("Laptop"),
)
workload = Workload()
workload.observe(query, frequency=100)
index = build_index(files, workload, memory_budget=10 * 1024 * 1024)
result = execute(files, query, index=index, workload=workload)
print(result.table.to_pylist())
print(result.stats)

# Offline adaptation: publish a completed replacement between workload epochs.
# The guide suggests 1,000 queries per epoch; refresh remains explicit.
index = build_index(files, workload, memory_budget=10 * 1024 * 1024)
```

The demo identifies a cross-column gap: `India` and `Laptop` occur separately in
one group but never together. That group is skipped before reading its data.

`Interval(20, 30)` represents an inclusive integer range. Open and one-sided
bounds, integer/string/boolean equality, and NULL comparison semantics from the
previous API are preserved. `Region()` matches all rows. Predicates are AND
conjunctions with one interval per column. Literals must match the Arrow column
type; integer overflow is rejected. Floats, timestamps, OR, casts, and arbitrary
SQL are outside certificate inference. Use the explicit ordinary DuckDB path:

```python
from certskip import execute_sql
result = execute_sql(files,
    "SELECT * FROM certskip_input WHERE age = 25 OR salary > 180000")
```

`execute_sql` accepts one SELECT, bypasses certificates, and returns an Arrow
Table. It is also the native DuckDB comparison path, separate from the adapter.
The typed `execute` API rejects unsupported predicates before pruning.

## Components

- `certskip/predicates.py`: typed query validation and SQL generation.
- `certskip/engine.py`: workload counters, bounded frequent candidate generation,
  file snapshots, ordinary integer min/max/all-null pruning, benefit estimates,
  packed index storage, and DuckDB integration.
- `cpp/model.hpp`, `pruning.cpp`: typed C++ intervals/regions and containment,
  including exact signed/unsigned 64-bit comparisons.
- `cpp/certificate.cpp`: exact verification against Arrow columns; a matching row
  rejects a candidate. NULL comparisons never match.
- `cpp/selector.cpp`: greedy marginal frequency-weighted compressed bytes saved
  per stored certificate byte. Each `(file, group, query)` benefit counts once.
- `cpp/parquet_reader.cpp`: selective Arrow C++ reads from a duplicate of the
  open snapshot descriptor. Verification projects only candidate columns.
- `cpp/bindings.cpp`: pybind11 bridge and Arrow table transfer to Python.

Candidates are bounded by `candidate_limit` (default 1,000), not a Cartesian
product of missing combinations. Wider regions can be seeded as workload queries.
Verification shares each projected row-group read across candidates, then checks
each candidate against its rows. Selection excludes benefits already obtained by
ordinary integer min/max or all-null pruning.

## Correctness and lifecycle

The invariant is `block ∩ certified_region = empty` and
`query_region ⊆ certified_region`. One complete certificate must cover the query;
a union of partial proofs is not used. Missing/stale/inapplicable certificates
fall back to ordinary reading. Statistics pruning runs first in both baseline
and certificate-enabled execution.

Version tokens contain filesystem device, inode, size, nanosecond modification
and change times. Build and execution hold open descriptors and compare tokens
before/after reads. Atomic replacement cannot switch an already-open native
reader; later operations ignore old certificates. An observed in-place mutation
raises `FileChangedError`, discarding the operation's result.

This assumes immutable local files, atomically replaced on a filesystem with
reliable timestamps. Tokens are not content hashes or remote-object versions;
concurrent in-place writers and cross-file transactional snapshots are unsupported.
`execute_sql` uses DuckDB's ordinary file access rather than this snapshot wrapper.

The index stores packed verified records in process. `memory_bytes` includes the
resident index object and payload. Build buffers, decoded records, query data,
and workload counters are separate. There is no untrusted certificate-loading
API. Modifying private index state invalidates the correctness contract.

## Validation and limits

Tests compare native containment with Python semantics and pruned answers with
both the certificate-disabled Arrow/DuckDB path and Python row evaluation. They
cover NULLs, integer extremes, duplicate preservation, overlapping benefits,
index budgets, file replacement, nested-column projection, ordinary pruning,
and skipping before native data reads.

`QueryStats` reports groups read/skipped, separates ordinary and certificate skips,
and reports **estimated** compressed bytes read/skipped. These are column-chunk
estimates, not measured filesystem I/O; footer reads are excluded. The adapter
materializes surviving groups before DuckDB filters them. Verification holds one
projected row group in memory. Index lookup is linear and selection is greedy;
this is a correctness-oriented prototype, not a native DuckDB optimizer extension.

Run a small repeated comparison (checks duplicates and records build/refresh cost):

```sh
.venv/bin/python -m examples.benchmark --repeats 5
```

This compares ordinary pruning (B0), a fixed trained index (B1), and an index
refreshed for a workload shift (B3), plus native DuckDB separately. It is a smoke
benchmark, not a full research evaluation: frequency-only B2, budget sweeps,
independent datasets and plotting remain future experiment work. No speedup or
novelty claim is made. The migration was locally tested with Python 3.9,
PyArrow 19.0.1, DuckDB 1.4.5 and pybind11 2.13.6 on macOS arm64.
