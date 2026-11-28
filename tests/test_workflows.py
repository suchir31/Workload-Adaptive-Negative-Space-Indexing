import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from certskip import (CertSkip, CertificateIndex, Interval as I, Region as R, Workload,
                      build_index, build_index_report, execute, load_index, save_index,
                      load_workload, save_workload)
from certskip.candidates import generate_candidates
from certskip.cli import main
from certskip.engine import _snapshot


@pytest.fixture
def data(tmp_path):
    path = tmp_path / "data.parquet"
    pq.write_table(pa.table({"x": list(range(10)), "y": list(range(10))}), path, row_group_size=5)
    return path


def workload(*queries):
    result = Workload()
    for q in queries:
        result.observe(q, 100)
    return result


def test_batch_verification_reaches_final_matching_row(tmp_path):
    path = tmp_path / "late.parquet"
    pq.write_table(pa.table({"x": [1] * 100 + [2], "y": [2] * 100 + [1]}), path)
    queries = [R.of(x=I.eq(2), y=I.eq(1)), R.of(x=I.eq(2), y=I.eq(2)), R()]
    with _snapshot(path) as (_, _, reader):
        for batch_size in (1, 7, 64, 65536):
            assert reader.verify(0, [q.to_dict() for q in queries], batch_size) == [False, True, False]
        with pytest.raises(ValueError):
            reader.verify(0, [queries[0].to_dict()], 0)
        with pytest.raises(ValueError):
            reader.verify(-1, [queries[0].to_dict()], 10)
    index = build_index([path], workload(*queries), batch_size=7)
    assert execute([path], queries[0], index).table.num_rows == 1


def test_merged_candidate_requires_full_hull_verification(tmp_path):
    path = tmp_path / "hull.parquet"
    pq.write_table(pa.table({"x": [0, 5, 10], "y": [0, 5, 10]}), path)
    a, b = R.of(x=I.eq(2), y=I.eq(2)), R.of(x=I.eq(8), y=I.eq(8))
    w = workload(a, b)
    candidates = generate_candidates(w, 10, "merged")
    hull = R.of(x=I(2, 8), y=I(2, 8))
    assert hull in candidates
    assert len(generate_candidates(w, 1, "merged")) == 1
    report = build_index_report([path], w, candidate_strategy="merged")
    assert report.stats.candidates == 3
    assert hull not in [c.region for c in report.index.certificates()]
    assert execute([path], R.of(x=I.eq(5), y=I.eq(5)), report.index).table.num_rows == 1


def test_merged_hull_can_cover_multiple_queries(tmp_path):
    path = tmp_path / "gap.parquet"
    pq.write_table(pa.table({"x": [0, 10], "y": [0, 10]}), path)
    w = workload(R.of(x=I.eq(2), y=I.eq(2)), R.of(x=I.eq(8), y=I.eq(8)))
    report = build_index_report([path], w, candidate_strategy="merged")
    assert len(report.index) == 1
    assert next(report.index.certificates()).region == R.of(x=I(2, 8), y=I(2, 8))
    assert report.stats.certificates_verified == 3
    assert report.stats.total_seconds >= report.stats.selection_seconds >= 0


def test_persistence_roundtrip_and_forged_proof(data, tmp_path):
    query = R.of(x=I.eq(1), y=I.eq(3))
    w = workload(query)
    wf, ix = tmp_path / "workload.json", tmp_path / "index.json"
    save_workload(w, wf)
    assert load_workload(wf).snapshot() == w.snapshot()
    original = build_index([data], w)
    save_index(original, ix)
    loaded = load_index(ix, [data], batch_size=1)
    assert len(loaded) == len(original) == 1
    assert execute([data], query, loaded).table.equals(execute([data], query).table)
    # Forge a region that contains a real row, retaining the valid current token.
    document = json.loads(ix.read_text())
    document["certificates"][0]["region"] = R.of(x=I.eq(1), y=I.eq(1)).to_dict()
    document["certificates"][0]["estimated_bytes"] = 10**100
    ix.write_text(json.dumps(document))
    loaded = load_index(ix, [data])
    assert len(loaded) == 0
    assert execute([data], R.of(x=I.eq(1)), loaded).table.num_rows == 1


def test_load_stale_budget_and_path_validation(data, tmp_path):
    q = R.of(x=I.eq(1), y=I.eq(3))
    saved = tmp_path / "index.json"
    save_index(build_index([data], workload(q)), saved)
    assert len(load_index(saved, [data], CertificateIndex().memory_bytes)) == 0
    with pytest.raises(ValueError, match="allowed paths"):
        load_index(saved, [tmp_path / "other.parquet"])
    with pytest.raises(ValueError, match="excessive"):
        load_index(saved, [data], max_records=0)
    pq.write_table(pa.table({"x": [1], "y": [3]}), data)
    assert len(load_index(saved, [data])) == 0
    assert execute([data], q, load_index(saved, [data])).table.num_rows == 1


def test_session_refresh_shift_and_failed_publication(data, monkeypatch):
    import certskip.session as module
    first = R.of(x=I.eq(1), y=I.eq(3))
    second = R.of(x=I.eq(6), y=I.eq(8))
    session = CertSkip([data], epoch_size=2, window_size=2)
    assert session.execute(first).stats.row_groups_skipped_by_certificates == 0
    session.execute(first)
    assert session.execute(first).stats.row_groups_skipped_by_certificates == 1
    assert session.refresh_count == 1
    session.execute(second)
    old_index = session.index
    original = module.build_index_report
    def fail(*args, **kwargs):
        raise RuntimeError("simulated failed refresh")
    monkeypatch.setattr(module, "build_index_report", fail)
    with pytest.raises(RuntimeError, match="failed refresh"):
        session.execute(second)
    assert session.index is old_index
    assert session.refresh_count == 1
    monkeypatch.setattr(module, "build_index_report", original)
    session.execute(second)
    session.execute(second)
    assert session.execute(second).stats.row_groups_skipped_by_certificates == 1
    assert set(dict(session.workload.snapshot())) == {second}
    assert all(c.region == second for c in session.index.certificates())


def test_frequency_selector_keeps_overlap(data):
    from certskip import _native
    benefits = [{0: 10, 1: 10}, {1: 10}, {2: 5}]
    assert _native.select([1, 1, 1], benefits, 2, True) == [0, 2]
    assert _native.select([1, 1, 1], benefits, 2, False) == [0, 1]
    q = R.of(x=I.eq(1), y=I.eq(3))
    report = build_index_report([data], workload(q), policy="frequency")
    assert report.stats.policy == "frequency"
    assert report.index.memory_bytes <= 10 * 1024 * 1024


def test_cli_end_to_end(tmp_path, capsys):
    directory = tmp_path / "generated"
    assert main(["generate", str(directory), "--groups", "4", "--group-size", "20"]) == 0
    path, saved = directory / "data.parquet", directory / "index.json"
    assert main(["build", str(path), "--workload", str(directory / "training.json"),
                 "--output", str(saved), "--batch-size", "3"]) == 0
    assert main(["query", str(path), "--region", str(directory / "training-query.json"),
                 "--index", str(saved), "--output", str(directory / "answer.parquet")]) == 0
    assert (directory / "answer.parquet").exists()
    assert main(["inspect", str(path)]) == 0
    assert main(["query", str(path), "--sql", "SELECT count(*) AS n FROM certskip_input"]) == 0
    assert json.loads(capsys.readouterr().out.splitlines()[-1])["rows"] == [{"n": 80}]
    with pytest.raises(SystemExit) as error:
        main(["query", str(path), "--sql", "SELECT 1", "--index", str(saved)])
    assert error.value.code == 2


def test_invalid_configuration_and_workload(data):
    with pytest.raises(TypeError):
        I(1, 2, "false", True)
    with pytest.raises(TypeError):
        Workload().observe("x = 1")
    for option in ({"epoch_size": 0}, {"window_size": True}, {"memory_budget": 0},
                   {"policy": "other"}, {"candidate_limit": -1}):
        with pytest.raises(ValueError):
            CertSkip([data], **option)
    with pytest.raises(ValueError):
        build_index([], Workload())
    with pytest.raises(ValueError):
        build_index([data], Workload(), batch_size=0)


def test_lazy_native_selector_matches_exhaustive_greedy():
    import random
    from fractions import Fraction
    from certskip import _native
    rng = random.Random(1234)
    for overlap in (False, True):
        for _ in range(100):
            costs = [rng.randrange(1, 20) for _ in range(20)]
            benefits = [{key: rng.randrange(1, 100) for key in range(15) if rng.random() < .3}
                        for _ in costs]
            budget = rng.randrange(1, 100)
            remaining, covered, expected, available = budget, set(), [], set(range(len(costs)))
            while available:
                best, score = None, Fraction(0)
                for i in sorted(available):
                    if costs[i] > remaining:
                        continue
                    gain = sum(v for k, v in benefits[i].items() if not overlap or k not in covered)
                    candidate = Fraction(gain, costs[i])
                    if candidate > score:
                        best, score = i, candidate
                if best is None:
                    break
                expected.append(best)
                covered.update(benefits[best])
                remaining -= costs[best]
                available.remove(best)
            assert _native.select(costs, benefits, budget, overlap) == expected


def test_streaming_checks_file_version_before_return(data, monkeypatch):
    import certskip.engine as engine
    from certskip import FileChangedError
    original = engine._read_group
    changed = False
    def mutate(reader, group):
        nonlocal changed
        table = original(reader, group)
        if not changed:
            with open(data, "ab") as handle:
                handle.write(b"changed")
            changed = True
        return table
    monkeypatch.setattr(engine, "_read_group", mutate)
    with pytest.raises(FileChangedError):
        execute([data], R())


def test_failed_query_is_not_observed(data):
    session = CertSkip([data], epoch_size=1)
    with pytest.raises(ValueError):
        session.execute(R.of(missing=I.eq(1)))
    assert session.workload.snapshot() == ()
    assert session.refresh_count == 0


def test_native_all_integer_widths_and_nulls(tmp_path):
    for dtype in (pa.int8(), pa.int16(), pa.int32(), pa.int64(),
                  pa.uint8(), pa.uint16(), pa.uint32(), pa.uint64()):
        path = tmp_path / (str(dtype) + ".parquet")
        pq.write_table(pa.table({"x": pa.array([0, 1, None, 2, 3], type=dtype),
                                "y": pa.array([3, 2, 1, None, 0], type=dtype)}), path)
        query = R.of(x=I(1, 2), y=I(1, 2))
        report = build_index_report([path], workload(query), batch_size=1)
        assert execute([path], query, report.index).table.to_pylist() == [{"x": 1, "y": 2}]


@pytest.mark.parametrize("document", [[], {}, {"format": "unknown"},
    {"format": "certskip-index-v1", "certificates": [None]},
    {"format": "certskip-index-v1", "certificates": "wrong"}])
def test_malformed_saved_indexes_fail_closed(data, tmp_path, document):
    path = tmp_path / "malformed.json"
    path.write_text(json.dumps(document))
    with pytest.raises(ValueError):
        load_index(path, [data])
