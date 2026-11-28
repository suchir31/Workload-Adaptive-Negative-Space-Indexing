"""Immutable-file certificate lifecycle and an Arrow C++ / DuckDB execution adapter."""
import json
import os
import sys
from collections import Counter
from contextlib import contextmanager, ExitStack
from dataclasses import dataclass
from pathlib import Path
from time import perf_counter

import pyarrow as pa
import pyarrow.parquet as pq
import duckdb

from . import _native

from .predicates import Region


class FileChangedError(RuntimeError):
    pass


def _version(handle):
    stat = os.fstat(handle.fileno())
    return (stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns)


@contextmanager
def _snapshot(path):
    # Keep this descriptor open: atomic path replacement cannot switch our reader.
    with open(path, "rb") as handle:
        version = _version(handle)
        parquet = pq.ParquetFile(handle)
        reader = _native.Reader(handle.fileno())
        yield parquet, version, reader
        if _version(handle) != version:
            raise FileChangedError("File changed during operation; discard result and retry")


class Workload:
    def __init__(self):
        self._counts = Counter()

    def observe(self, query: Region, frequency: int = 1):
        if not isinstance(query, Region):
            raise TypeError("Expected a Region query")
        if type(frequency) is not int or frequency <= 0:
            raise ValueError("Frequency must be a positive integer")
        self._counts[query] += frequency

    def snapshot(self):
        return tuple(self._counts.items())

    def candidates(self, limit=1000):
        if type(limit) is not int or limit < 0:
            raise ValueError("Candidate limit must be a nonnegative integer")
        return tuple(query for query, _ in self._counts.most_common(limit))


@dataclass(frozen=True)
class Certificate:
    path: str
    version: tuple
    row_group: int
    region: Region
    estimated_bytes: int

    def encode(self):
        return json.dumps(dict(path=self.path, version=self.version,
                               row_group=self.row_group, region=self.region.to_dict(),
                               estimated_bytes=self.estimated_bytes),
                          sort_keys=True, separators=(",", ":")).encode() + b"\n"

    @classmethod
    def decode(cls, data):
        data = json.loads(data)
        data["version"] = tuple(data["version"])
        data["region"] = Region.from_dict(data["region"])
        return cls(**data)


class CertificateIndex:
    """Packed, resident certificate storage. Only build() populates trusted proofs.

    Budget covers this object's shallow storage and its packed bytes. Temporary
    decoded records, workload, verification and query buffers are not index memory.
    Persistence re-verifies every loaded proposal before creating trusted proofs.
    """
    __slots__ = ("_payload",)

    def __init__(self):
        self._payload = b""

    @property
    def memory_bytes(self):
        return sys.getsizeof(self) + sys.getsizeof(self._payload)

    def certificates(self):
        # Stream decoding avoids retaining a second resident object index.
        start = 0
        while start < len(self._payload):
            end = self._payload.index(b"\n", start)
            yield Certificate.decode(self._payload[start:end])
            start = end + 1

    def __len__(self):
        return self._payload.count(b"\n")

    def matching_groups(self, path, version, query):
        """Decode once per file/query, rather than once per row group.

        The returned set is transient query state, outside resident index storage.
        """
        native_query = query.to_dict()
        return {c.row_group for c in self.certificates()
                if c.path == path and c.version == version and
                _native.covers(c.region.to_dict(), native_query)}

    def prunes(self, path, version, row_group, query):
        return any(c.path == path and c.version == version and
                   c.row_group == row_group and _native.covers(c.region.to_dict(), query.to_dict())
                   for c in self.certificates())


def _compressed_bytes(metadata):
    return sum(metadata.column(i).total_compressed_size
               for i in range(metadata.num_columns))


def _stats_exclude(metadata, query):
    """Conservative cost baseline for integer min/max and all-null columns.

    Used by both ordinary pruning and the selector savings baseline.
    """
    columns = {metadata.column(i).path_in_schema: metadata.column(i)
               for i in range(metadata.num_columns)}
    paths = [metadata.column(i).path_in_schema for i in range(metadata.num_columns)]
    for name, bound in query.terms:
        if paths.count(name) != 1:
            continue  # Dotted flat names can collide with nested leaf paths.
        column = columns.get(name)
        stats = column.statistics if column is not None else None
        if stats is None:
            continue
        if stats.has_null_count and stats.null_count == metadata.num_rows:
            return True
        if not stats.has_min_max or type(stats.min) is not int:
            continue
        if bound.lower is not None and type(bound.lower) is int:
            if stats.max < bound.lower or (stats.max == bound.lower and not bound.lower_closed):
                return True
        if bound.upper is not None and type(bound.upper) is int:
            if stats.min > bound.upper or (stats.min == bound.upper and not bound.upper_closed):
                return True
    return False


@dataclass(frozen=True)
class BuildStats:
    candidates: int
    row_groups_verified: int
    certificates_verified: int
    certificates_selected: int
    index_bytes: int
    verification_seconds: float
    selection_seconds: float
    total_seconds: float
    policy: str


@dataclass(frozen=True)
class BuildResult:
    index: CertificateIndex
    stats: BuildStats


def build_index_report(paths, workload, memory_budget=10 * 1024 * 1024,
                       candidate_limit=1000, *, policy="overlap", batch_size=65536,
                       candidate_strategy="queries"):
    """Verify workload candidates exactly, then greedily select marginal savings/byte.

    Rebuild with a new workload snapshot to adapt. Candidate generation is bounded
    by distinct frequent queries, not the Cartesian product of attribute domains.
    """
    started = perf_counter()
    if policy not in ("overlap", "frequency"):
        raise ValueError("Policy must be overlap or frequency")
    if type(batch_size) is not int or batch_size <= 0:
        raise ValueError("Batch size must be a positive integer")
    paths = sorted({str(Path(p).resolve()) for p in paths})
    if not paths:
        raise ValueError("At least one Parquet file is required")
    index = CertificateIndex()
    if type(memory_budget) is not int or memory_budget < index.memory_bytes:
        raise ValueError("Budget must fit the empty index: %d bytes" % index.memory_bytes)
    queries = workload.snapshot()
    from .candidates import generate_candidates
    candidates = generate_candidates(workload, candidate_limit, candidate_strategy)
    groups_verified = 0
    verified = []
    coverage = []
    benefit_ids = {}
    for path in paths:
        with _snapshot(path) as (parquet, version, reader):
            for query in candidates + tuple(q for q, _ in queries):
                query.validate(parquet.schema_arrow)
            native_candidates = [candidate.to_dict() for candidate in candidates]
            for group_id in range(parquet.num_row_groups):
                metadata = parquet.metadata.row_group(group_id)
                verified_empty = reader.verify(group_id, native_candidates, batch_size)
                groups_verified += bool(candidates)
                remaining = {candidate for candidate, empty in zip(candidates, verified_empty) if empty}
                for candidate in candidates:
                    if candidate not in remaining:
                        continue
                    certificate = Certificate(path, version, group_id, candidate,
                                              _compressed_bytes(metadata))
                    benefits = {
                        (path, group_id, q): frequency * certificate.estimated_bytes
                        for q, frequency in queries
                        if _native.covers(candidate.to_dict(), q.to_dict()) and not _stats_exclude(metadata, q)
                    }
                    verified.append(certificate.encode())
                    coverage.append({
                        benefit_ids.setdefault(key, len(benefit_ids)): value
                        for key, value in benefits.items()
                    })
    # Native selector counts each (block, query) benefit once across overlapping proofs.
    verification_done = perf_counter()
    selected = _native.select([len(record) for record in verified], coverage,
                              memory_budget - index.memory_bytes, policy == "overlap")
    index._payload = b"".join(verified[i] for i in selected)
    finished = perf_counter()
    return BuildResult(index, BuildStats(
        len(candidates), groups_verified, len(verified), len(index), index.memory_bytes,
        verification_done - started, finished - verification_done, finished - started, policy))


def build_index(paths, workload, memory_budget=10 * 1024 * 1024, candidate_limit=1000, **options):
    """Build a verified index; use build_index_report for timings and counts."""
    return build_index_report(paths, workload, memory_budget, candidate_limit, **options).index


@dataclass(frozen=True)
class QueryStats:
    row_groups_read: int
    row_groups_skipped: int
    estimated_bytes_skipped: int
    row_groups_skipped_by_statistics: int = 0
    row_groups_skipped_by_certificates: int = 0
    estimated_bytes_read: int = 0
    elapsed_seconds: float = 0.0
    pruning_seconds: float = 0.0
    read_seconds: float = 0.0


@dataclass(frozen=True)
class QueryResult:
    table: pa.Table
    stats: QueryStats


def _read_group(reader, group_id):
    return reader.read_row_group(group_id)


def execute(paths, query: Region, index=None, workload=None):
    """Stream surviving row groups to DuckDB after safe ordinary/certificate pruning.

    Input buffering is one row group plus DuckDB's own operator buffers. The
    returned output table is materialized. All file snapshots remain open until
    the SQL execution finishes and version checks succeed.
    """
    started = perf_counter()
    files = sorted({str(Path(p).resolve()) for p in paths})
    if not files:
        raise ValueError("At least one Parquet file is required")
    counts = dict(read=0, skipped=0, saved=0, ordinary=0, certificates=0,
                  bytes_read=0, pruning_seconds=0.0, read_seconds=0.0)
    with ExitStack() as stack:
        snapshots = [(path, stack.enter_context(_snapshot(path))) for path in files]
        schema = snapshots[0][1][0].schema_arrow
        for _, (parquet, _, _) in snapshots:
            query.validate(parquet.schema_arrow)
            if not schema.equals(parquet.schema_arrow):
                raise ValueError("All files must have the same Arrow schema")

        def batches():
            for path, (parquet, version, reader) in snapshots:
                planning_started = perf_counter()
                certified = index.matching_groups(path, version, query) if index is not None else set()
                counts["pruning_seconds"] += perf_counter() - planning_started
                for group_id in range(parquet.num_row_groups):
                    planning_started = perf_counter()
                    metadata = parquet.metadata.row_group(group_id)
                    ordinary = _stats_exclude(metadata, query)
                    certificate = not ordinary and group_id in certified
                    counts["pruning_seconds"] += perf_counter() - planning_started
                    if ordinary or certificate:
                        counts["skipped"] += 1
                        counts["ordinary"] += int(ordinary)
                        counts["certificates"] += int(certificate)
                        counts["saved"] += _compressed_bytes(metadata)
                    else:
                        read_started = perf_counter()
                        table = _read_group(reader, group_id)
                        counts["read_seconds"] += perf_counter() - read_started
                        counts["read"] += 1
                        counts["bytes_read"] += _compressed_bytes(metadata)
                        yield from table.to_batches(max_chunksize=65536)
                        del table

        stream = pa.RecordBatchReader.from_batches(schema, batches())
        with duckdb.connect() as connection:
            connection.register("certskip_input", stream)
            result = connection.execute(
                "SELECT * FROM certskip_input WHERE " + query.sql()).fetch_arrow_table().cast(schema)
    if workload is not None:
        workload.observe(query)
    return QueryResult(result, QueryStats(
        counts["read"], counts["skipped"], counts["saved"], counts["ordinary"],
        counts["certificates"], counts["bytes_read"], perf_counter() - started,
        counts["pruning_seconds"], counts["read_seconds"]))


def execute_sql(paths, sql):
    """Run arbitrary read-only SQL against `certskip_input` using native DuckDB.

    This is the explicit fallback/reference for predicates outside Region's scope.
    It does not use certificates and does not report adapter row-group counters.
    """
    files = sorted({str(Path(p).resolve()) for p in paths})
    if not files:
        raise ValueError("At least one Parquet file is required")
    with duckdb.connect() as connection:
        statements = connection.extract_statements(sql)
        if len(statements) != 1 or statements[0].type != duckdb.StatementType.SELECT:
            raise ValueError("Expected a single SELECT statement")
        connection.read_parquet(files).create_view("certskip_input")
        return connection.execute(sql).fetch_arrow_table()
