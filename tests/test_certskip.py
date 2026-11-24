import os
import random

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from certskip import CertificateIndex, Interval as I, Region as R, Workload, build_index, execute
from certskip.engine import FileChangedError, _snapshot


def write(path, rows, row_group_size=3):
    table = pa.Table.from_pylist(rows)
    pq.write_table(table, path, row_group_size=row_group_size)
    return table


@pytest.fixture
def dataset(tmp_path):
    path = tmp_path / "data.parquet"
    write(path, [
        {"country": "USA", "product": "Laptop", "age": 25},
        {"country": "USA", "product": "Phone", "age": 40},
        {"country": "India", "product": "Phone", "age": None},
        {"country": "India", "product": "Laptop", "age": 25},
        {"country": None, "product": "Laptop", "age": 30},
        {"country": "USA", "product": None, "age": 20},
    ])
    return path


def test_joint_gap_prunes_only_empty_group(dataset):
    q = R.of(country=I.eq("India"), product=I.eq("Laptop"))
    workload = Workload()
    workload.observe(q, 10)
    index = build_index([dataset], workload)
    baseline = execute([dataset], q)
    result = execute([dataset], q, index, workload)
    assert result.table.equals(baseline.table)
    assert result.table.num_rows == 1
    assert result.stats.row_groups_read == result.stats.row_groups_skipped == 1
    assert result.stats.estimated_bytes_skipped > 0
    assert dict(workload.snapshot())[q] == 11
    assert len(index) == 1


def test_all_skipped_preserves_schema(dataset):
    q = R.of(country=I.eq("Germany"))
    w = Workload()
    w.observe(q)
    result = execute([dataset], q, build_index([dataset], w))
    assert result.stats.row_groups_read == 0
    assert result.stats.row_groups_skipped == 2
    assert result.table.num_rows == 0
    assert result.table.schema == pq.read_schema(dataset)


def test_containment_and_boundaries():
    cert = R.of(age=I(20, 30), salary=I(150000, 200000))
    assert cert.covers(R.of(age=I.eq(25), salary=I.eq(180000)))
    assert not cert.covers(R.of(age=I(20, 40), salary=I.eq(180000)))
    assert not cert.covers(R.of(age=I.eq(25)))
    assert not I(20, 30, False, False).contains(I.eq(20))
    assert not I(20, 30, False, False).contains(I.eq(30))
    assert I(20, 30).contains(I(20, 30, False, False))
    assert I(20, None).contains(I(21, 100))
    assert not I(20, 30).contains(I(20, None))
    assert not I.eq(True).contains(I.eq(1))
    assert not I.eq(2).contains_value(None)


def test_wider_query_not_pruned(dataset):
    cert = R.of(country=I.eq("India"), product=I.eq("Laptop"))
    w = Workload()
    w.observe(cert)
    q = R.of(country=I.eq("India"))
    index = build_index([dataset], w)
    result = execute([dataset], q, index)
    assert result.table.equals(execute([dataset], q).table)
    assert result.stats.row_groups_skipped == 0


def test_replacement_invalidates(dataset, tmp_path):
    q = R.of(country=I.eq("Germany"))
    w = Workload()
    w.observe(q)
    index = build_index([dataset], w)
    replacement = tmp_path / "replacement.parquet"
    write(replacement, [{"country": "Germany", "product": "Laptop", "age": 25}])
    os.replace(replacement, dataset)
    result = execute([dataset], q, index)
    assert result.table.num_rows == 1
    assert result.stats.row_groups_skipped == 0


def test_in_place_rewrite_invalidates(dataset):
    q = R.of(country=I.eq("Germany"))
    w = Workload()
    w.observe(q)
    index = build_index([dataset], w)
    write(dataset, [{"country": "Germany", "product": "Laptop", "age": 25}])
    assert execute([dataset], q, index).table.num_rows == 1


def test_concurrent_mutation_detected(dataset):
    with pytest.raises(FileChangedError):
        with _snapshot(dataset):
            with open(dataset, "ab") as handle:
                handle.write(b"changed")


def test_budget_and_marginal_overlap(dataset):
    broad = R.of(country=I.eq("Germany"))
    narrow = R.of(country=I.eq("Germany"), product=I.eq("Laptop"))
    w = Workload()
    w.observe(broad, 100)
    w.observe(narrow, 1)
    full = build_index([dataset], w)
    assert len(full) == 2  # One broad proof per row group; redundant narrow proofs add no benefit.
    empty_size = CertificateIndex().memory_bytes
    assert len(build_index([dataset], w, empty_size)) == 0
    budget = empty_size + len(next(full.certificates()).encode())
    small = build_index([dataset], w, budget)
    assert len(small) == 1
    assert small.memory_bytes <= budget
    with pytest.raises(ValueError):
        build_index([dataset], w, empty_size - 1)
    assert not build_index([dataset], w, candidate_limit=0)


def test_integer_stats_baseline(tmp_path):
    path = tmp_path / "ints.parquet"
    write(path, [{"age": 10}, {"age": 20}])
    w = Workload()
    w.observe(R.of(age=I.eq(100)))
    assert len(build_index([path], w)) == 0  # Existing min/max already proves absence.


def test_sql_escaping_and_nulls(tmp_path):
    path = tmp_path / "quotes.parquet"
    name = 'odd"column'
    q = R(((name, I.eq("a'b")),))
    write(path, [{name: "a'b"}, {name: None}, {name: "other"}])
    assert execute([path], q).table.to_pylist() == [{name: "a'b"}]


def test_invalid_types_rejected(dataset):
    with pytest.raises(TypeError):
        execute([dataset], R.of(age=I.eq("25")))
    with pytest.raises(ValueError):
        execute([dataset], R.of(missing=I.eq(1)))
    for args in [(None, None), (1.2, 2.3), (True, 1), (30, 20)]:
        with pytest.raises(ValueError):
            I(*args)
    with pytest.raises(ValueError):
        I.eq(None)
    with pytest.raises(ValueError):
        R((("age", I.eq(1)), ("age", I.eq(2))))


def test_randomized_differential(tmp_path):
    rng = random.Random(71)
    path = tmp_path / "random.parquet"
    rows = [{"x": rng.choice([None, 0, 1, 2, 3, 4, 5]),
             "y": rng.choice([None, "a", "b", "c"])} for _ in range(90)]
    write(path, rows, row_group_size=5)
    queries = []
    for _ in range(60):
        low = rng.randrange(-1, 6)
        high = rng.randrange(low + 1, 8)
        queries.append(R.of(x=I(low, high, rng.choice([True, False]),
                              rng.choice([True, False])),
                            y=I.eq(rng.choice(["a", "b", "c", "d"]))))
    w = Workload()
    for q in queries:
        w.observe(q, rng.randrange(1, 20))
    index = build_index([path], w, memory_budget=10000)
    assert index.memory_bytes <= 10000
    assert len(index) > 0
    for q in queries + [R(), R.of(x=I(0, None))]:
        result = execute([path], q, index)
        assert result.table.equals(execute([path], q).table)
        assert result.table.to_pylist() == [row for row in rows if q.matches(row)]


def test_empty_file_and_multiple_files(tmp_path, dataset):
    empty = tmp_path / "empty.parquet"
    pq.write_table(pa.Table.from_batches([], schema=pq.read_schema(dataset)), empty)
    w = Workload()
    q = R.of(country=I.eq("India"))
    w.observe(q)
    index = build_index([empty, dataset], w)
    assert execute([empty, dataset], q, index).table.equals(execute([dataset], q).table)
    assert execute([empty], R()).table.num_rows == 0
    with pytest.raises(ValueError):
        execute([], R())


def test_skip_happens_before_row_group_read(dataset, monkeypatch):
    q = R.of(country=I.eq("India"), product=I.eq("Laptop"))
    w = Workload()
    w.observe(q)
    index = build_index([dataset], w)
    calls = []
    original = pq.ParquetFile.read_row_group

    def tracked(self, group, *args, **kwargs):
        calls.append(group)
        return original(self, group, *args, **kwargs)

    monkeypatch.setattr(pq.ParquetFile, "read_row_group", tracked)
    execute([dataset], q, index)
    assert calls == [1]


def test_integer_extremes_boolean_and_range_gap(tmp_path):
    path = tmp_path / "typed.parquet"
    pq.write_table(pa.table({
        "x": pa.array([-(2**63), 2**63 - 1, None], type=pa.int64()),
        "u": pa.array([0, 2**64 - 1, None], type=pa.uint64()),
        "flag": pa.array([True, False, None], type=pa.bool_()),
    }), path)
    queries = [R.of(x=I.eq(-(2**63))), R.of(u=I.eq(2**64 - 1)),
               R.of(flag=I.eq(True)), R.of(x=I(-10, 10))]
    w = Workload()
    for q in queries:
        w.observe(q)
    index = build_index([path], w)
    assert len(index) == 1  # Range gap is inside the min/max envelope.
    for q in queries:
        assert execute([path], q, index).table.equals(execute([path], q).table)
    with pytest.raises((OverflowError, pa.ArrowInvalid)):
        execute([path], R.of(x=I.eq(2**63)))


def test_workload_change_reselects(dataset):
    first = R.of(country=I.eq("Germany"))
    second = R.of(country=I.eq("Japan"))
    w = Workload()
    w.observe(first, 100)
    w.observe(second)
    full = build_index([dataset], w)
    budget = CertificateIndex().memory_bytes + max(len(c.encode()) for c in full.certificates())
    initial = build_index([dataset], w, budget)
    assert next(initial.certificates()).region == first
    w.observe(second, 1000)
    adapted = build_index([dataset], w, budget)
    assert next(adapted.certificates()).region == second
