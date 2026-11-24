"""Immutable-file certificate lifecycle and a DataFusion execution adapter."""
import json
import os
import sys
from collections import Counter
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from datafusion import SessionContext

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
        yield parquet, version
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
                   c.row_group == row_group and c.region.covers(query)
                   for c in self.certificates())


def _compressed_bytes(metadata):
    return sum(metadata.column(i).total_compressed_size
               for i in range(metadata.num_columns))


def _stats_exclude(metadata, query):
    """Conservative cost baseline for integer min/max and all-null columns.

    This affects selection value only; it is never used as an emptiness proof.
    """
    columns = {metadata.column(i).path_in_schema: metadata.column(i)
               for i in range(metadata.num_columns)}
    for name, bound in query.terms:
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
    for path in sorted({str(Path(p).resolve()) for p in paths}):
        with _snapshot(path) as (parquet, version):
            for query, _ in queries:
                query.validate(parquet.schema_arrow)
            columns = sorted({name for candidate in candidates for name, _ in candidate.terms})
            for group_id in range(parquet.num_row_groups):
                metadata = parquet.metadata.row_group(group_id)
                # An empty conjunction is TRUE; only an empty block can certify it.
                remaining = set(candidates)
                if columns:
                    for batch in parquet.iter_batches(row_groups=[group_id], columns=columns):
                        for row in batch.to_pylist():
                            remaining.difference_update(c for c in tuple(remaining) if c.matches(row))
                        if not remaining:
                            break
                elif metadata.num_rows:
                    remaining.clear()
                for candidate in candidates:
                    if candidate not in remaining:
                        continue
                    certificate = Certificate(path, version, group_id, candidate,
                                              _compressed_bytes(metadata))
                    benefits = {
                        (path, group_id, q): frequency * certificate.estimated_bytes
                        for q, frequency in queries
                        if candidate.covers(q) and not _stats_exclude(metadata, q)
                    }
                    verified.append(certificate.encode())
                    coverage.append(benefits)
    # Each (block, query) benefit may be claimed once, even with overlapping regions.
    covered = set()
    available = set(range(len(verified)))
    selected = []
    used = index.memory_bytes
    while available:
        best, best_score = None, 0
        for i in sorted(available):
            if used + len(verified[i]) > memory_budget:
                continue
            gain = sum(value for key, value in coverage[i].items() if key not in covered)
            score = gain / len(verified[i])
            if score > best_score:
                best, best_score = i, score
        if best is None:
            break
        selected.append(verified[best])
        used += len(verified[best])
        covered.update(coverage[best])
        available.remove(best)
    index._payload = b"".join(selected)
    return index


@dataclass(frozen=True)
class QueryStats:
    row_groups_read: int
    row_groups_skipped: int
    estimated_bytes_skipped: int


@dataclass(frozen=True)
class QueryResult:
    table: pa.Table
    stats: QueryStats


def execute(paths, query: Region, index=None, workload=None):
    """Prune before reading row-group data, then run the exact predicate in DataFusion.

    The same typed Region drives containment and the SQL filter. Arbitrary SQL is
    intentionally not accepted as a separate filter that could disagree with it.
    """
    tables = []
    schema = None
    read = skipped = saved = 0
    for path in sorted({str(Path(p).resolve()) for p in paths}):
        with _snapshot(path) as (parquet, version):
            query.validate(parquet.schema_arrow)
            if schema is None:
                schema = parquet.schema_arrow
            elif not schema.equals(parquet.schema_arrow):
                raise ValueError("All files must have the same Arrow schema")
            for group_id in range(parquet.num_row_groups):
                if index is not None and index.prunes(path, version, group_id, query):
                    skipped += 1
                    saved += _compressed_bytes(parquet.metadata.row_group(group_id))
                else:
                    tables.append(parquet.read_row_group(group_id))
                    read += 1
    if schema is None:
        raise ValueError("At least one Parquet file is required")
    table = pa.concat_tables(tables) if tables else pa.Table.from_batches([], schema=schema)
    context = SessionContext()
    # Explicit empty batch retains the schema even if all row groups were skipped.
    batches = table.to_batches() or [pa.RecordBatch.from_arrays(
        [pa.array([], type=field.type) for field in schema], schema=schema)]
    context.register_record_batches("certskip_input", [batches])
    output = context.sql("SELECT * FROM certskip_input WHERE " + query.sql()).collect()
    result = pa.Table.from_batches(output) if output else pa.Table.from_batches([], schema=schema)
    if workload is not None:
        workload.observe(query)
    return QueryResult(result, QueryStats(read, skipped, saved))
