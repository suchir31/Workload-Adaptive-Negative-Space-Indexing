# CertSkip v0.2 — Python / C++

The beginner guide is the design reference for this revision. This checkout
previously implemented a Python/DataFusion prototype, not a Rust crate.

## Architecture and interfaces

- Python owns the public `Interval`, `Region`, `Workload`, `build_index`, and
  `execute` APIs, workload collection, file lifecycle, and packed certificates.
- C++20 owns typed region containment, exact Arrow-table verification, greedy
  marginal-benefit selection, and selective Arrow C++ Parquet row-group reads.
- pybind11 connects Python to C++. PyArrow supplies matching Arrow headers and
  shared libraries; DuckDB filters the selected Arrow table using SQL generated
  from the same validated Region used for pruning.
- A block is `(canonical path, file version, row-group number)`. A certificate
  adds a verified region and estimated compressed size. Regions are conjunctions
  of typed intervals. The existing integer ranges, string/boolean equalities and
  SQL NULL semantics remain supported.
- A native Reader duplicates the already-open snapshot descriptor. Certificate
  verification and execution therefore read the same file version as metadata.
- Ordinary integer min/max and all-null pruning precedes certificate checks.
  Unsupported SQL can use `execute_sql`, a native DuckDB scan without certificates.

## Correctness and scope

Every certificate requires exhaustive verification. Pruning requires one whole
certificate region to contain the query. File identity and timestamps are checked
before/after each operation; these are immutable-local-file version tokens, not
content hashes. Files must be atomically replaced, not concurrently modified.
Index budgets cover packed resident certificates; build/query buffers are separate.
Refresh is explicit and offline, between batches of queries (suggested size 1,000).

## Validation and measurements

Build the native extension, run predicate/budget/file-lifecycle differential tests,
and run the cross-column-gap demo. Compare certificate-disabled/enabled execution
on the same Arrow/DuckDB path; native DuckDB is a separate reference. Benchmarks
must check identical rows including duplicates, repeat at least five times, and
label compressed-byte estimates. No speedup or research novelty is assumed.

This is the implementation plan for the requested migration, not a claim of team
review or approval. Distributed execution, live updates, arbitrary SQL certificate
inference, and persisted untrusted certificates remain outside scope.
