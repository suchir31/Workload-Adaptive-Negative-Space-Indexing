# CertSkip

A working prototype of workload-adaptive, exactly verified empty-region
certificates for immutable Parquet files. Row groups are the pruning unit.
PyArrow reads only the selected row groups, then Apache DataFusion executes the
query predicate over their Arrow batches.

## Run

Python 3.9 or later:

```sh
python3 -m venv .venv
.venv/bin/python -m pip install -e '.[test]'
.venv/bin/python -m pytest -q
.venv/bin/python -m examples.demo
```

The demo finds a cross-column gap: `India` and `Laptop` both occur in the first
row group, but never together. It skips that group, reads the second group, and
returns the same matching row as an unpruned query.

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

# Adapt to observations collected by execute(); publish the completed new index.
index = build_index(files, workload, memory_budget=10 * 1024 * 1024)
```

A numeric region can use `Interval(20, 30)`,
`Interval(20, 30, lower_closed=False)`, or `Interval(20, None)`.
`Region()` matches all rows. Predicates are conjunctions with one interval per
column. Equality supports Arrow integer, UTF-8 string, and boolean columns;
ranges support integers. NULL values do not satisfy comparisons. Literal types
must match the column, and integer overflow is rejected. Floats, decimals,
timestamps, OR, casts, and arbitrary SQL are deliberately outside this first
predicate language; callers must use their ordinary query engine for those
queries. Unsupported predicates raise an error before any pruning.

## Components

- **Workload analyzer:** counts observed typed predicates. Explicit frequencies
  support imported workloads. A query can record itself after successful execution.
- **Candidate generator:** proposes the most frequent distinct query regions,
  bounded by `candidate_limit` (default 1,000). It never enumerates missing
  Cartesian products. Wider regions can be seeded as workload predicates.
- **Verifier:** scans candidate columns in batches for each row group. A single
  matching row rejects a candidate; only exhaustive absence creates a certificate.
  All candidates share this verification pass. NULLs follow the query language's
  SQL comparison semantics.
- **Selector:** greedily chooses highest marginal frequency-weighted compressed
  bytes saved per certificate byte. It counts each `(file, row group, query)`
  benefit once. Integer min/max and all-null metadata reduce the estimated
  benefit of proofs ordinary statistics already provide. Selection is a heuristic,
  not an exact knapsack optimizer.
- **Index:** packs selected certificates into bytes. Each record includes the
  canonical path, file-version token, row-group ID, typed empty region, and cost
  estimate. `memory_bytes` includes the resident index object and packed payload;
  the configured budget is enforced against that amount. Workload counters,
  build-time candidate buffers, temporary decoding, and query data are separate.
- **Execution:** checks `query implies certificate region` before calling
  `read_row_group`. Missing, stale, or inapplicable certificates cause a read.
  The exact same `Region` generates the DataFusion SQL filter, avoiding a
  potentially unsound separate caller-supplied SQL/pruning-predicate pair.

## File lifecycle and correctness

The invariant is `block ∩ certified_region = empty` and
`query_region ⊆ certified_region`. Therefore skipped blocks contain no query
matches. A union of several certificates is not used to infer coverage.

Version tokens contain filesystem device, inode, size, nanosecond modification
time, and nanosecond change time. Build and execution each keep an open file
descriptor and compare its token before and after reading. Atomic replacement
cannot switch an already-open reader; subsequent queries see the replacement's
new token and ignore old certificates. An observed in-place mutation raises
`FileChangedError`, discarding the operation's result.

This contract assumes immutable local files replaced atomically on a filesystem
with reliable identity and timestamps. It is not a content hash, remote object
versioning scheme, or a transaction protocol for concurrent in-place writers.
Multiple files are opened independently, so cross-file transactional snapshots
are not provided. Rebuild after replacement to restore pruning benefits.

Certificates are created in-process through verification. There is no API to
load untrusted or hand-written certificate files; mutating private index state
invalidates the correctness contract.

## Scope and measurements

This is a Python scan adapter, not a native DataFusion optimizer rule or Rust
TableProvider. Surviving row groups are materialized in memory before DataFusion
filters them. The API returns all columns for a typed selection query; it does
not provide arbitrary SQL aggregation or join planning. Files in one query must
have matching schemas.

The packed index currently performs a linear certificate lookup per row group.
Candidate verification and greedy selection favor clarity over large-workload
scalability. Selection estimates omit lookup CPU costs and do not model all
Parquet bloom-filter, page-index, string-statistics, or projection savings.
`estimated_bytes_skipped` sums compressed column-chunk sizes; it is not measured
filesystem I/O and excludes footer reads. The unpruned reference uses the same
adapter with certificate pruning disabled, not DataFusion's native Parquet scan.

Tests cover exact cross-column absence, containment boundaries, NULLs, SQL
escaping, budget enforcement, overlapping benefits, file invalidation, empty
files, and randomized comparison with both unpruned DataFusion and Python
predicate evaluation.

The integration uses DataFusion's documented
[record-batch registration API](https://datafusion.apache.org/python/autoapi/datafusion/context/index.html).
Dependencies target DataFusion 43 and PyArrow 18–19 for the Python 3.9 environment;
the implementation was tested with DataFusion 43.1.0 and PyArrow 19.0.1.
