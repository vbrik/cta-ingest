import os
import pytest
from pathlib import Path
import cta_ingest

PART_SIZE = 50_000  # 50 KB — small enough to get multiple chunks from 200 KB random data


def make_origin_entry(path: Path, filename: str, size: int = 200_000) -> dict:
    """Create a random (incompressible) file and return an origin.json entry for it."""
    fp = path / filename
    fp.write_bytes(os.urandom(size))
    stat = fp.stat()
    return {
        "path": str(fp),
        "size": stat.st_size,
        "mtime": stat.st_mtime,
        "atime": stat.st_atime,
    }


def test_disassemble_dry_run(s3w, work_dir, origin_dir):
    entry = make_origin_entry(origin_dir, "run.fits")
    s3w.put_as_json({"run.fits": entry}, "origin.json")
    s3w.put_as_json({}, "target.json")
    cta_ingest.disassemble(s3w, work_dir, PART_SIZE, dry_run=True)
    assert not any(work_dir.iterdir())
    assert s3w.get_from_json("disassemble.json", default={}) == {}


def test_disassemble_produces_multiple_chunks(s3w, work_dir, origin_dir):
    entry = make_origin_entry(origin_dir, "run.fits")
    s3w.put_as_json({"run.fits": entry}, "origin.json")
    s3w.put_as_json({}, "target.json")
    cta_ingest.disassemble(s3w, work_dir, PART_SIZE, dry_run=False)
    state = s3w.get_from_json("disassemble.json")
    assert "run.fits" in state
    chunk_paths = state["run.fits"]
    assert len(chunk_paths) >= 2
    assert all(Path(p).exists() for p in chunk_paths)
    # Total compressed size should be in the same ballpark as the original (random = incompressible)
    total = sum(Path(p).stat().st_size for p in chunk_paths)
    assert total >= 190_000


def test_disassemble_skips_already_processed(s3w, work_dir, origin_dir):
    entry = make_origin_entry(origin_dir, "already.fits")
    s3w.put_as_json({"already.fits": entry}, "origin.json")
    s3w.put_as_json({}, "target.json")
    s3w.put_as_json({"already.fits": ["/fake/chunk"]}, "disassemble.json")
    cta_ingest.disassemble(s3w, work_dir, PART_SIZE, dry_run=False)
    state = s3w.get_from_json("disassemble.json")
    assert state["already.fits"] == ["/fake/chunk"]
    assert not (work_dir / "already.fits").exists()


def test_disassemble_cleanup_delivered(s3w, work_dir, origin_dir):
    filename = "delivered.fits"
    entry = make_origin_entry(origin_dir, filename)
    s3w.put_as_json({filename: entry}, "origin.json")
    s3w.put_as_json({filename: ["dummy_chunk"]}, "disassemble.json")
    s3w.put_as_json({filename: entry}, "target.json")  # delivered
    chunk_dir = work_dir / filename
    chunk_dir.mkdir()
    (chunk_dir / "aa").write_bytes(b"x")
    cta_ingest.disassemble(s3w, work_dir, PART_SIZE, dry_run=False)
    assert not chunk_dir.exists()
    assert filename not in s3w.get_from_json("disassemble.json", default={})


def test_disassemble_missing_target_raises(s3w, work_dir, origin_dir):
    entry = make_origin_entry(origin_dir, "f.fits")
    s3w.put_as_json({"f.fits": entry}, "origin.json")
    with pytest.raises(cta_ingest.NoSuchKeyError):
        cta_ingest.disassemble(s3w, work_dir, PART_SIZE, dry_run=False)


def test_disassemble_mixed_state(s3w, work_dir, origin_dir):
    """A single call handles new, in-progress, and delivered files together."""
    new_entry = make_origin_entry(origin_dir, "new.fits")
    inprog_entry = make_origin_entry(origin_dir, "inprog.fits")
    delivered_entry = make_origin_entry(origin_dir, "delivered.fits")
    s3w.put_as_json(
        {"new.fits": new_entry, "inprog.fits": inprog_entry, "delivered.fits": delivered_entry},
        "origin.json",
    )
    s3w.put_as_json({"delivered.fits": delivered_entry}, "target.json")
    s3w.put_as_json(
        {"inprog.fits": ["/fake/inprog_chunk"], "delivered.fits": ["/fake/delivered_chunk"]},
        "disassemble.json",
    )
    delivered_chunk_dir = work_dir / "delivered.fits"
    delivered_chunk_dir.mkdir()
    (delivered_chunk_dir / "aa").write_bytes(b"x")

    cta_ingest.disassemble(s3w, work_dir, PART_SIZE, dry_run=False)

    state = s3w.get_from_json("disassemble.json")
    assert "new.fits" in state
    assert all(Path(p).exists() for p in state["new.fits"])
    assert state["inprog.fits"] == ["/fake/inprog_chunk"]
    assert "delivered.fits" not in state
    assert not delivered_chunk_dir.exists()
