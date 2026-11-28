"""Atomic JSON persistence. Loaded certificates are proposals, never trusted proofs."""
import json
import os
from pathlib import Path
import tempfile

from .engine import Certificate, CertificateIndex, Workload, _snapshot, _compressed_bytes
from .predicates import Region


def _write_json(path, document):
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=target.parent,
                                         prefix=target.name + ".", suffix=".tmp", delete=False) as handle:
            temporary = handle.name
            json.dump(document, handle, separators=(",", ":"), sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, target)
    finally:
        if temporary and os.path.exists(temporary):
            os.unlink(temporary)


def _read_json(path, max_bytes=64 * 1024 * 1024):
    with open(path, "rb") as handle:
        data = handle.read(max_bytes + 1)
    if len(data) > max_bytes:
        raise ValueError("JSON file exceeds the size limit")
    return json.loads(data)


def save_workload(workload, path):
    _write_json(path, {"format": "certskip-workload-v1", "queries": [
        {"region": q.to_dict(), "frequency": frequency} for q, frequency in workload.snapshot()]})


def load_workload(path):
    data = _read_json(path)
    if not isinstance(data, dict) or data.get("format") != "certskip-workload-v1":
        raise ValueError("Unsupported workload format")
    queries = data.get("queries")
    if not isinstance(queries, list):
        raise ValueError("Workload queries must be a list")
    workload = Workload()
    for item in queries:
        if not isinstance(item, dict) or not {"region", "frequency"} <= item.keys():
            raise ValueError("Invalid workload entry")
        workload.observe(Region.from_dict(item["region"]), item["frequency"])
    return workload


def save_index(index, path):
    _write_json(path, {"format": "certskip-index-v1", "certificates": [
        json.loads(c.encode()) for c in index.certificates()]})


def load_index(path, paths, memory_budget=10 * 1024 * 1024, *, batch_size=65536,
               max_records=100000):
    """Reverify saved records against explicitly allowed immutable files.

    Stale records are discarded. Matching version tokens do not establish trust:
    every remaining proposed region is scanned again. File paths outside `paths`
    are rejected. A modified JSON file can never introduce an unchecked proof.
    The returned index may be smaller because of staleness, failed proofs or budget.
    """
    index = CertificateIndex()
    if type(memory_budget) is not int or memory_budget < index.memory_bytes:
        raise ValueError("Budget must fit the empty index")
    if type(batch_size) is not int or batch_size <= 0:
        raise ValueError("Batch size must be a positive integer")
    if type(max_records) is not int or max_records < 0:
        raise ValueError("Record limit must be nonnegative")
    allowed = {str(Path(p).resolve()) for p in paths}
    if not allowed:
        raise ValueError("At least one Parquet file is required")
    data = _read_json(path)
    if not isinstance(data, dict) or data.get("format") != "certskip-index-v1":
        raise ValueError("Unsupported index format")
    records = data.get("certificates")
    if not isinstance(records, list) or len(records) > max_records:
        raise ValueError("Invalid or excessive certificate records")
    by_path = {}
    for record in records:
        if not isinstance(record, dict) or not {"path", "row_group", "version", "region"} <= record.keys():
            raise ValueError("Invalid certificate record")
        if not isinstance(record["path"], str):
            raise ValueError("Certificate path must be a string")
        if record["path"] not in allowed:
            raise ValueError("Certificate references a file outside the allowed paths")
        group = record["row_group"]
        version = record["version"]
        if type(group) is not int or group < 0:
            raise ValueError("Invalid row-group number")
        if not isinstance(version, list) or len(version) != 5 or any(type(v) is not int for v in version):
            raise ValueError("Invalid file-version token")
        region = Region.from_dict(record["region"])
        by_path.setdefault(record["path"], []).append((tuple(version), group, region))
    selected, used = [], index.memory_bytes
    for source in sorted(by_path):
        with _snapshot(source) as (parquet, version, reader):
            groups = {}
            for token, group, region in by_path[source]:
                if token != version:
                    continue
                if group >= parquet.num_row_groups:
                    raise ValueError("Row group does not exist")
                region.validate(parquet.schema_arrow)
                groups.setdefault(group, set()).add(region)
            for group, regions in sorted(groups.items()):
                regions = sorted(regions, key=lambda r: json.dumps(r.to_dict(), sort_keys=True))
                empty = reader.verify(group, [r.to_dict() for r in regions], batch_size)
                for region, verified in zip(regions, empty):
                    if not verified:
                        continue
                    record = Certificate(source, version, group, region,
                                         _compressed_bytes(parquet.metadata.row_group(group))).encode()
                    if used + len(record) <= memory_budget:
                        selected.append(record)
                        used += len(record)
    index._payload = b"".join(selected)
    return index
