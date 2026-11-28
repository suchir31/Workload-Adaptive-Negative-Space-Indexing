# CertSkip

CertSkip is a working Python/C++20 system for workload-adaptive negative-space
indexing on immutable local Parquet files. It verifies that multidimensional
regions contain no matching rows, then skips their row groups before reading data.
Arrow C++ performs selective reads; DuckDB executes SQL over the surviving data.

Version 0.3 includes automatic adaptation, bounded candidate generation, batched
native verification, overlap-aware selection, safe persistence, a CLI, and
reproducible B0–B3 experiments. See [PROJECT_PLAN.md](PROJECT_PLAN.md) for scope.

## Install

Python 3.9–3.13, a C++20 compiler, and macOS 12+ or Linux are required. PyArrow
19.0.1 supplies the matching Arrow headers and shared libraries. The POSIX native
reader does not support Windows. No Rust or DataFusion installation is needed.

```sh
python3 -m venv .venv
.venv/bin/python -m pip install --upgrade pip
.venv/bin/python -m pip install -e '.[test,plot]'
.venv/bin/python -m pytest -q
.venv/bin/python -m examples.demo
```

`plot` is optional. Rebuild the editable installation after changing C++ sources.
For CMake development and standalone native tests:

```sh
.venv/bin/python -m pip install cmake pybind11
.venv/bin/cmake -S . -B build/cmake -DPython_EXECUTABLE="$PWD/.venv/bin/python"
.venv/bin/cmake --build build/cmake --parallel 2
.venv/bin/ctest --test-dir build/cmake --output-on-failure
```

## Try the complete command-line workflow

Commands are available as `.venv/bin/certskip` or `.venv/bin/python -m certskip`.
Generated data and results directories are ignored by Git.

```sh
.venv/bin/certskip generate data/demo --groups 20 --group-size 100
.venv/bin/certskip inspect data/demo/data.parquet
.venv/bin/certskip build data/demo/data.parquet \
  --workload data/demo/training.json --budget 8192 \
  --candidate-strategy merged --output data/demo/index.json
.venv/bin/certskip query data/demo/data.parquet \
  --region data/demo/training-query.json --index data/demo/index.json
.venv/bin/certskip query data/demo/data.parquet \
  --sql 'SELECT count(*) FROM certskip_input WHERE x < 20 OR y > 80'
```

Use `query --output answer.parquet` for large results. Index loading re-verifies
saved certificates, so repeated CLI queries pay that validation cost. Use a
long-lived Python session to reuse an already-verified index without reloading.

## Adaptive Python session

```python
from certskip import CertSkip, Interval, Region, Workload

query = Region.of(x=Interval(20, 30), y=Interval(60, 70))
training = Workload()
training.observe(query, frequency=100)

session = CertSkip(
    ["data/demo/data.parquet"],
    memory_budget=8192,
    epoch_size=1000,
    window_size=5000,
    candidate_limit=1000,
    candidate_strategy="merged",
)
report = session.train(training)  # Optional; an untrained session starts with no certificates.
result = session.execute(query)
print(result.table.to_pylist())
print(result.stats)
print(report.stats)
```

Only successful typed queries enter the bounded recent-query window. After an
epoch, the next query first refreshes from that window. Old observations age out,
so changing workloads can replace obsolete certificates. `session.refresh()`
forces an offline refresh; `session.last_build` and `session.refresh_count` expose
its cost and history. A completed replacement is published atomically. If a
refresh fails, the old index and pending observations survive and the call raises
before executing the next query. Calls on one session are serialized.

The original API remains available:

```python
from certskip import build_index, build_index_report, execute

index = build_index(files, workload, memory_budget=8192)
result = execute(files, query, index=index, workload=workload)
report = build_index_report(files, workload, memory_budget=8192,
                            batch_size=4096, policy="overlap")
```

The cumulative `Workload` object is suitable for explicit training. `CertSkip`
adds a sliding window for automatic adaptation. Imported training does not fill
the recent-query window; subsequent successful queries determine future refreshes.

## Predicates and candidates

`Region` is an AND conjunction of one `Interval` per column. Integers support
closed, open and one-sided bounds, including signed/unsigned 64-bit extremes.
Strings and booleans support equality. NULLs never satisfy comparisons. Literals
must match the Arrow type, and overflow is rejected. `Region()` matches every row.

`candidate_strategy="queries"` uses the most frequent observed regions.
`"merged"` reserves space for bounding rectangles joining compatible frequent
regions, with at most `4 * candidate_limit` pair attempts. These broader regions
can cover several queries, but every candidate still requires exact verification;
absence at two endpoints never implies absence in the middle. Candidate count
never exceeds `candidate_limit` (default 1,000).

Floats, timestamps, OR, joins, aggregation and arbitrary SQL are outside certificate
inference. Use `session.execute_sql(sql)` or `execute_sql(files, sql)` for the
ordinary native DuckDB path. SQL references `certskip_input` and must be one SELECT.
It never uses certificates or feeds unsupported predicates into the workload.
Typed results retain source Arrow schema metadata; row ordering is not guaranteed.

## Safe index and workload persistence

```python
from certskip import save_index, load_index, save_workload, load_workload

save_workload(workload, "workload.json")
workload = load_workload("workload.json")
save_index(index, "index.json")
index = load_index("index.json", files, memory_budget=8192)
```

Writes use a flushed temporary file and atomic replacement. JSON formats are
versioned. Workloads contain `queries` entries with `region` and positive integer
`frequency`; regions use `Region.to_dict()`. The CLI generator writes examples.

Loaded certificates are untrusted proposals. Paths must belong to the explicitly
supplied file list; stale records are discarded, remaining regions are validated
and rescanned, and costs are recomputed from current metadata. A forged region
cannot become a proof even if its file token matches. Failed proofs and records
that do not fit the byte budget are omitted. Default loading limits are 64 MiB
of JSON and 100,000 records. Reverification is intentionally not a zero-I/O reload.

## Implementation and correctness

- **Native verification:** projected Parquet columns arrive in configurable Arrow
  record batches. Direct typed-array access avoids per-cell scalar allocations.
  A matching row rejects a candidate; only exhausting the group establishes absence.
  Candidates already disproved are removed from subsequent batch checks.
- **Native selection:** lazy greedy marginal frequency-weighted bytes saved per
  certificate byte. Scores are recomputed as coverage changes, with deterministic
  ties. The `frequency` comparison policy ignores overlapping benefits.
- **Pruning:** ordinary integer min/max and all-null checks precede certificates.
  C++ containment requires one whole proof region to contain the query. A query
  decodes the packed index once per file and uses a transient set of skippable groups.
- **Execution:** surviving row groups stream to DuckDB through Arrow rather than
  accumulating the entire input table first. The final output table is materialized.

The invariant is `block ∩ certified_region = empty` and
`query_region ⊆ certified_region`. Combining partial certificates is not used to
infer coverage. Missing, stale, or inapplicable proofs fall back to ordinary reads.

Build and typed execution hold snapshot descriptors. The native reader duplicates
that descriptor, so metadata, verification and data reads refer to the same file.
Device/inode/size/nanosecond modification/change times identify file versions and
are checked before returning results. Detected mutation raises `FileChangedError`.
Atomic replacement cannot redirect an open reader; later calls ignore old proofs.

This requires immutable local files on filesystems with reliable identity and
timestamps. Tokens are not content hashes. Concurrent in-place writers, remote
object stores and cross-file transactional snapshots are outside the contract.
The SQL fallback uses DuckDB's ordinary file access rather than the snapshot wrapper.

## Metrics and experiments

`BuildStats` reports candidates, groups verified, verified/selected certificate
counts, index size, preparation/verification time, selection time and total time.
`QueryStats` reports ordinary/certificate skips, groups read, estimated compressed
bytes, native read time, pruning time and total elapsed time. Byte counts sum
column-chunk metadata; they are **estimates**, exclude footers, and are not measured
filesystem I/O. Pruning/read times are portions of total time, not extra costs to add.

```sh
.venv/bin/certskip benchmark --repeats 5 --budgets 2048 8192 \
  --output results/benchmark.json --plot results/benchmark.png
```

The runner checks exact row multisets including duplicates for every measured
query, warms up each variant, interleaves variants in a seeded shuffled order,
and records individual samples, p50/p95 latency, reads, estimated bytes, index
size and build/refresh costs. It tests correlated and independent data, multiple
budgets, and initial/shifted workloads. Each variant receives the same byte budget.

| Variant | Behavior |
| --- | --- |
| B0 | Arrow/DuckDB path with ordinary pruning only |
| B1 | Fixed overlap-aware index trained on the initial workload |
| B2 | Refreshed frequency-weighted selection, ignoring overlapping benefits |
| B3 | Refreshed overlap-aware selection |
| DuckDB | Separate native Parquet/SQL reference |

Refreshes use each phase's supplied training workload before timed queries;
refresh cost is reported separately. This measures offline adaptation, not an
online predictor or end-to-end amortized speedup. The automatic session path has
separate workload-shift and failure-recovery tests. No novelty or general speedup
claim is made from synthetic measurements.

## Resource scope and validation

The index budget covers its resident object and packed payload. Candidate/coverage
buffers, workload observations, temporary query skip sets, file metadata, Arrow
buffers and output data are separate. Verification batches bound decoded rows per
batch, not a hard process-memory limit. Query input buffering is one row group
plus DuckDB/Arrow internal buffers; all file descriptors remain open until the
query completes. Index lookup is still linear once per file/query.

Tests cover native/Python containment, all integer widths, NULLs, escaped names,
late matches across batches, overlap selection against an exhaustive reference,
file invalidation, mutation during streaming, budgets, persistence tampering,
candidate hulls, adaptive refresh failures, and the CLI. CI builds and tests the
Python and C++ paths on Linux/macOS. This is the complete local research-system
scope, not a distributed database or a native DuckDB optimizer extension.
