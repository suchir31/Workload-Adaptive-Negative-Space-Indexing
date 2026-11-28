# CertSkip v0.3 — complete local Python/C++ system

The supplied beginner guide defines the local-file research-system scope. The
previous v0.2 migration replaced Python/DataFusion with C++20/Arrow/DuckDB;
v0.3 completes the operational and experimental workflows.

## Implemented

- Typed integer ranges and string/boolean equalities, AND regions, SQL NULL
  semantics, exact signed/unsigned boundaries, and ordinary SQL fallback.
- Bounded frequent-query and merged-rectangle candidates; native projected,
  batched verification with early rejection and direct typed-array access.
- Native deterministic lazy greedy overlap-aware selection under a packed-index
  byte budget; frequency-weighted selection without overlap adjustment for B2.
- Snapshot-bound native row-group reads, conservative ordinary metadata pruning,
  native certificate containment, and streaming Arrow-to-DuckDB execution.
- Sliding-window workload tracking, epoch-triggered refresh, failure-atomic index
  publication and explicit offline training through `CertSkip` sessions.
- Atomic versioned JSON workload/index persistence. Loaded certificates must be
  reverified; stale tokens and failed proofs never enable skipping.
- Build/query timing, read/skip counters and explicitly estimated compressed bytes.
- CLI for generation, inspection, training, typed/SQL querying and benchmarking.
- B0/B1/B2/B3 plus native DuckDB evaluation on correlated/independent data, budget
  sweeps, workload shifts, at least five repetitions and optional static plots.
- Native tests, differential/integration tests, CLI tests and Linux/macOS CI.

## Interfaces and ownership

`Interval`, `Region`, `Workload`, `build_index`, `build_index_report`, `execute`,
`execute_sql`, `CertSkip`, and persistence functions are public Python APIs.
Python owns lifecycle, orchestration, workload collection, candidate proposals,
metadata pruning and packed storage. C++ owns verification, greedy selection,
containment and selective Arrow reads. pybind11 and matching PyArrow libraries
bridge the languages; DuckDB executes the same validated predicate used for pruning.

`BuildResult` separates its `index` from `BuildStats`, so reporting overhead does
not consume index budget. `QueryResult` holds the returned Arrow table and stats.
Refreshes fully build a replacement before assignment; failed refreshes retain
both the prior index and pending observations. The automatic window defaults to
5,000 successful queries, with refresh every 1,000.

## Correctness contract and exclusions

Every proof establishes row-group absence; every skip requires full containment.
Only immutable local files, atomically replaced on reliable filesystems, are
supported. Version tokens are filesystem identity/timestamps, not content hashes.
Index files are never trusted without scanning their proposed regions again.

Index bytes exclude build buffers, metadata, workloads, query buffers and output.
Verification batches limit decoded rows, not total process memory. Typed execution
streams row groups but materializes its result. General SQL always uses ordinary
DuckDB and does not learn certificates. Distributed execution, remote stores,
concurrent in-place writers, cross-file transactions, SQL predicate inference and
zero-I/O trusted persistence are outside this release.

## Evaluation interpretation

B0 uses ordinary pruning; B1 keeps initial certificates; B2 and B3 refresh on the
same supplied phase workload with and without overlap accounting. All operate
under identical configured budgets. Native DuckDB is a separate reference.
Experiments validate row multisets including duplicates, warm up and shuffle
variants, retain raw timing samples, and report refresh costs separately from
query latency. These are offline synthetic experiments, not evidence of universal
speedup or research novelty. Automatic session adaptation is tested separately.
