import pytest
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


@pytest.mark.xfail(
    strict=True,
    reason="Source bug: _run_pipeline does not inspect cmd1's returncode, so an "
           "upstream failure (e.g. zstd crashing mid-stream) is silently swallowed. "
           "Fix by waiting on p1 and raising when p1.returncode != 0.",
)
def test_run_pipeline_cmd1_nonzero_should_raise():
    with pytest.raises(Exception):
        cta_ingest._run_pipeline(["false"], ["cat"])


# _rmdir_recursive is a misnomer: it unlinks leaf files only and will raise
# IsADirectoryError if the target contains nested subdirectories. Its callers
# in cta-ingest.py only pass flat chunk directories, which matches this contract.
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
