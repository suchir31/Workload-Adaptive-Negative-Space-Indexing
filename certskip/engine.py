"""Immutable-file certificate lifecycle and an Arrow C++ / DuckDB execution adapter."""
import json
import os
import sys
from collections import Counter
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

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
    No load-from-disk API: unchecked external certificates must never become proofs.
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


def build_index(paths, workload, memory_budget=10 * 1024 * 1024, candidate_limit=1000):
    """Verify workload candidates exactly, then greedily select marginal savings/byte.

    Rebuild with a new workload snapshot to adapt. Candidate generation is bounded
    by distinct frequent queries, not the Cartesian product of attribute domains.
    """
    index = CertificateIndex()
    if type(memory_budget) is not int or memory_budget < index.memory_bytes:
        raise ValueError("Budget must fit the empty index: %d bytes" % index.memory_bytes)
    queries = workload.snapshot()
    candidates = workload.candidates(candidate_limit)
    verified = []
    coverage = []
    benefit_ids = {}
    for path in sorted({str(Path(p).resolve()) for p in paths}):
        with _snapshot(path) as (parquet, version, reader):
            for query, _ in queries:
                query.validate(parquet.schema_arrow)
            native_candidates = [candidate.to_dict() for candidate in candidates]
            for group_id in range(parquet.num_row_groups):
                metadata = parquet.metadata.row_group(group_id)
                verified_empty = reader.verify(group_id, native_candidates)
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
    selected = _native.select([len(record) for record in verified], coverage,
                              memory_budget - index.memory_bytes)
    index._payload = b"".join(verified[i] for i in selected)
    return index


@dataclass(frozen=True)
class QueryStats:
    row_groups_read: int
    row_groups_skipped: int
    estimated_bytes_skipped: int
    row_groups_skipped_by_statistics: int = 0
    row_groups_skipped_by_certificates: int = 0
    estimated_bytes_read: int = 0


@dataclass(frozen=True)
class QueryResult:
    table: pa.Table
    stats: QueryStats


def _read_group(reader, group_id):
    return reader.read_row_group(group_id)


def execute(paths, query: Region, index=None, workload=None):
    """Prune before reading row-group data, then run the exact predicate in DuckDB.

    The same typed Region drives containment and the SQL filter. Arbitrary SQL is
    intentionally not accepted as a separate filter that could disagree with it.
    """
    tables = []
    schema = None
    read = skipped = saved = stats_skipped = cert_skipped = bytes_read = 0
    for path in sorted({str(Path(p).resolve()) for p in paths}):
        with _snapshot(path) as (parquet, version, reader):
            query.validate(parquet.schema_arrow)
            if schema is None:
                schema = parquet.schema_arrow
            elif not schema.equals(parquet.schema_arrow):
                raise ValueError("All files must have the same Arrow schema")
            for group_id in range(parquet.num_row_groups):
                metadata = parquet.metadata.row_group(group_id)
                ordinary_skip = _stats_exclude(metadata, query)
                certificate_skip = not ordinary_skip and index is not None and index.prunes(
                    path, version, group_id, query)
                if ordinary_skip or certificate_skip:
                    skipped += 1
                    stats_skipped += int(ordinary_skip)
                    cert_skipped += int(bool(certificate_skip))
                    saved += _compressed_bytes(metadata)
                else:
                    tables.append(_read_group(reader, group_id))
                    read += 1
                    bytes_read += _compressed_bytes(metadata)
    if schema is None:
        raise ValueError("At least one Parquet file is required")
    table = pa.concat_tables(tables) if tables else pa.Table.from_batches([], schema=schema)
    with duckdb.connect() as connection:
        connection.register("certskip_input", table)
        result = connection.execute("SELECT * FROM certskip_input WHERE " + query.sql()).fetch_arrow_table()
    # DuckDB can normalize Arrow metadata/nullability; retain the source contract.
    result = result.cast(schema)
    if workload is not None:
        workload.observe(query)
    return QueryResult(result, QueryStats(read, skipped, saved, stats_skipped,
                                         cert_skipped, bytes_read))


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
