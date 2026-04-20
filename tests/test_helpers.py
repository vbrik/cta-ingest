import pytest
from pathlib import Path
import cta_ingest


def test_run_pipeline_data_flows(tmp_path):
    out = tmp_path / "out.txt"
    cta_ingest._run_pipeline(["printf", "abc"], ["tee", str(out)])
    assert out.read_bytes() == b"abc"


def test_run_pipeline_cmd2_nonzero():
    with pytest.raises(Exception) as exc_info:
        cta_ingest._run_pipeline(["echo", "hi"], ["false"])
    args = exc_info.value.args
    assert args[0] == "NonZeroReturnCode"
    assert args[1] == 1


def test_rmdir_recursive_removes_files_and_dir(tmp_path):
    d = tmp_path / "mydir"
    d.mkdir()
    for name in ("a", "b", "c"):
        (d / name).write_bytes(b"x")
    cta_ingest._rmdir_recursive(d)
    assert not d.exists()


def test_rmdir_recursive_empty_dir(tmp_path):
    d = tmp_path / "emptydir"
    d.mkdir()
    cta_ingest._rmdir_recursive(d)
    assert not d.exists()
