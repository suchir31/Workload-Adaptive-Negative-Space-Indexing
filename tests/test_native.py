"""Regression tests across the C++/Arrow/Python/DuckDB boundary."""
import os
import random

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from certskip import Interval as I, Region as R, Workload, build_index, execute, execute_sql
from certskip import _native
from certskip.engine import _snapshot


def test_native_containment_matches_python():
    rng = random.Random(512)
    bounds = [I.eq(True), I.eq(False), I.eq("a"), I.eq("b"),
              I.eq(-(2**63)), I.eq(2**64 - 1)]
    for _ in range(100):
        a = rng.randrange(-100, 100)
        bounds.append(I(a, a + rng.randrange(1, 50), rng.choice([True, False]),
                        rng.choice([True, False])))
    for _ in range(2000):
        a, b = rng.choice(bounds), rng.choice(bounds)
        outer, inner = R.of(x=a), R.of(x=b)
        assert _native.covers(outer.to_dict(), inner.to_dict()) == outer.covers(inner)


def test_ordinary_pruning_avoids_native_reads(tmp_path, monkeypatch):
    from certskip import engine
    path = tmp_path / "ordinary.parquet"
    pq.write_table(pa.table({"x": pa.array([1, 2, None, None], type=pa.int64())}),
                   path, row_group_size=2)
    def unexpected(*args):
        pytest.fail("Excluded row groups must not be read")
    monkeypatch.setattr(engine, "_read_group", unexpected)
    result = execute([path], R.of(x=I(2, None, lower_closed=False)))
    assert result.table.num_rows == 0
    assert result.stats.row_groups_skipped_by_statistics == 2
    assert result.stats.row_groups_skipped_by_certificates == 0
    assert result.stats.estimated_bytes_read == 0


def test_native_reader_keeps_open_snapshot(tmp_path):
    path, replacement = tmp_path / "data.parquet", tmp_path / "new.parquet"
    pq.write_table(pa.table({"x": [1, 2]}), path)
    pq.write_table(pa.table({"x": [9]}), replacement)
    # Replacement can change the old inode's ctime on some filesystems; the
    # lifecycle check may reject it, but the native reader must never switch data.
    from certskip import FileChangedError
    try:
        with _snapshot(path) as (_, _, reader):
            os.replace(replacement, path)
            assert reader.read_row_group(0).to_pylist() == [{"x": 1}, {"x": 2}]
    except FileChangedError:
        pass
    assert execute([path], R()).table.to_pylist() == [{"x": 9}]


def test_nested_projection_and_dotted_metadata_collision(tmp_path):
    path = tmp_path / "nested.parquet"
    table = pa.table({"nested": [{"a": 100, "b": 200}, {"a": 100, "b": 200}],
                      "nested.a": [1, 2], "y": [2, 1]})
    pq.write_table(table, path)
    q = R.of(**{"nested.a": I.eq(1), "y": I.eq(1)})
    workload = Workload()
    workload.observe(q)
    index = build_index([path], workload)
    assert len(index) == 1
    assert execute([path], q, index).stats.row_groups_skipped_by_certificates == 1
    # Nested metadata must not incorrectly exclude a matching flat dotted column.
    assert execute([path], R.of(**{"nested.a": I.eq(1)})).table.num_rows == 1


def test_sql_fallback_preserves_duplicates(tmp_path):
    path = tmp_path / "data.parquet"
    pq.write_table(pa.table({"x": [1, 1, 2, 3], "y": [1.5, 1.5, 2.5, None]}), path)
    output = execute_sql([path], "SELECT * FROM certskip_input WHERE x = 1 OR y > 2 ORDER BY x")
    assert output.to_pylist() == [{"x": 1, "y": 1.5}, {"x": 1, "y": 1.5}, {"x": 2, "y": 2.5}]
    with pytest.raises(ValueError):
        execute_sql([path], "DROP TABLE certskip_input")
    with pytest.raises(ValueError):
        execute_sql([path], "SELECT 1; SELECT 2")


def test_native_selector_overlap_and_budget():
    assert _native.select([1, 1, 1], [{0: 5, 1: 5}, {1: 5, 2: 1}, {3: 4}], 2) == [0, 2]
    assert _native.select([5], [{0: 10}], 4) == []
    with pytest.raises(ValueError):
        _native.select([0], [{0: 10}], 4)
