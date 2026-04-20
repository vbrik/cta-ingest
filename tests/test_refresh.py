import pytest
import cta_ingest


def test_refresh_origin_basic(s3w, origin_dir):
    (origin_dir / "scan.txt").write_bytes(b"x")
    (origin_dir / "run.fits").write_bytes(b"y" * 100)
    cta_ingest.refresh_terminus(s3w, origin_dir, excludes=[], my_state_key="origin.json")
    result = s3w.get_from_json("origin.json")
    assert set(result.keys()) == {"scan.txt", "run.fits"}
    assert result["scan.txt"]["size"] == 1
    assert result["run.fits"]["size"] == 100
    assert result["scan.txt"]["path"] == str(origin_dir / "scan.txt")
    for key in ("mtime", "atime", "ts"):
        assert key in result["scan.txt"]


def test_refresh_excludes(s3w, origin_dir):
    (origin_dir / "_hidden.txt").write_bytes(b"h")
    (origin_dir / "visible.txt").write_bytes(b"v")
    cta_ingest.refresh_terminus(s3w, origin_dir, excludes=["_"], my_state_key="origin.json")
    result = s3w.get_from_json("origin.json")
    assert "visible.txt" in result
    assert "_hidden.txt" not in result


def test_refresh_multiple_excludes(s3w, origin_dir):
    (origin_dir / ".dot").write_bytes(b"d")
    (origin_dir / "_under").write_bytes(b"u")
    (origin_dir / "normal.txt").write_bytes(b"n")
    cta_ingest.refresh_terminus(s3w, origin_dir, excludes=[".", "_"], my_state_key="origin.json")
    result = s3w.get_from_json("origin.json")
    assert list(result.keys()) == ["normal.txt"]


def test_refresh_overwrites_state(s3w, origin_dir):
    (origin_dir / "file_a.txt").write_bytes(b"a")
    cta_ingest.refresh_terminus(s3w, origin_dir, excludes=[], my_state_key="origin.json")
    (origin_dir / "file_a.txt").unlink()
    (origin_dir / "file_b.txt").write_bytes(b"b")
    cta_ingest.refresh_terminus(s3w, origin_dir, excludes=[], my_state_key="origin.json")
    result = s3w.get_from_json("origin.json")
    assert list(result.keys()) == ["file_b.txt"]


def test_refresh_empty_dir(s3w, origin_dir):
    cta_ingest.refresh_terminus(s3w, origin_dir, excludes=[], my_state_key="origin.json")
    assert s3w.get_from_json("origin.json") == {}


def test_refresh_ignores_subdirectories(s3w, origin_dir):
    (origin_dir / "file.txt").write_bytes(b"x")
    (origin_dir / "nested").mkdir()
    (origin_dir / "nested" / "inner.txt").write_bytes(b"y")
    cta_ingest.refresh_terminus(s3w, origin_dir, excludes=[], my_state_key="origin.json")
    result = s3w.get_from_json("origin.json")
    assert list(result.keys()) == ["file.txt"]


def test_show_status_all_delivered(s3w, capsys):
    origin = {"a.fits": {"size": 100}, "b.fits": {"size": 200}}
    target = {"a.fits": {"size": 100}, "b.fits": {"size": 200}}
    s3w.put_as_json(origin, "origin.json")
    s3w.put_as_json(target, "target.json")
    cta_ingest.show_status(s3w)
    out = capsys.readouterr().out
    assert "Present: 2" in out
    assert "Undelivered: []" in out
    assert "Mismatched: []" in out


def test_show_status_some_undelivered(s3w, capsys):
    origin = {"a.fits": {"size": 100}, "b.fits": {"size": 200}}
    target = {"a.fits": {"size": 100}}
    s3w.put_as_json(origin, "origin.json")
    s3w.put_as_json(target, "target.json")
    cta_ingest.show_status(s3w)
    out = capsys.readouterr().out
    assert "Present: 1" in out
    assert "Undelivered: ['b.fits']" in out
    assert "Mismatched: []" in out


def test_show_status_size_mismatch(s3w, capsys):
    origin = {"a.fits": {"size": 100}}
    target = {"a.fits": {"size": 999}}
    s3w.put_as_json(origin, "origin.json")
    s3w.put_as_json(target, "target.json")
    cta_ingest.show_status(s3w)
    out = capsys.readouterr().out
    assert "Present: 1" in out
    assert "Undelivered: []" in out
    assert "Mismatched: ['a.fits']" in out


def test_show_status_missing_target_raises(s3w):
    s3w.put_as_json({"a.fits": {"size": 1}}, "origin.json")
    with pytest.raises(cta_ingest.NoSuchKeyError):
        cta_ingest.show_status(s3w)


def test_show_status_missing_origin_raises(s3w):
    s3w.put_as_json({"a.fits": {"size": 1}}, "target.json")
    with pytest.raises(cta_ingest.NoSuchKeyError):
        cta_ingest.show_status(s3w)
