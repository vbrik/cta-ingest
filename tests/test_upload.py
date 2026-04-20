import os
import pytest
from pathlib import Path
import cta_ingest


def make_chunk_dir(work_dir: Path, filename: str, num_chunks: int = 2, chunk_size: int = 1000):
    """Create real chunk files on disk and return their absolute path strings."""
    chunk_dir = work_dir / filename
    chunk_dir.mkdir(parents=True)
    paths = []
    for i in range(num_chunks):
        p = chunk_dir / f"chunk_{i:02d}"
        p.write_bytes(os.urandom(chunk_size))
        paths.append(str(p))
    return paths


def test_upload_dry_run(s3w, work_dir):
    paths = make_chunk_dir(work_dir, "file.fits")
    s3w.put_as_json({"file.fits": paths}, "disassemble.json")
    s3w.put_as_json({}, "target.json")
    cta_ingest.upload(s3w, dry_run=True)
    assert s3w.list_keys(prefix="parts") == []
    assert s3w.get_from_json("upload.json", default={}) == {}


def test_upload_uploads_parts(s3w, work_dir):
    paths = make_chunk_dir(work_dir, "file.fits", num_chunks=3)
    s3w.put_as_json({"file.fits": paths}, "disassemble.json")
    s3w.put_as_json({}, "target.json")
    cta_ingest.upload(s3w, dry_run=False)
    state = s3w.get_from_json("upload.json")
    assert "file.fits" in state
    assert len(state["file.fits"]) == 3
    assert all(k.startswith("parts") for k in state["file.fits"])
    assert set(s3w.list_keys(prefix="parts")) == set(state["file.fits"])


def test_upload_key_naming(s3w, work_dir):
    # Key is literally 'parts' + absolute_path_string
    paths = make_chunk_dir(work_dir, "file.fits", num_chunks=1)
    s3w.put_as_json({"file.fits": paths}, "disassemble.json")
    s3w.put_as_json({}, "target.json")
    cta_ingest.upload(s3w, dry_run=False)
    state = s3w.get_from_json("upload.json")
    assert state["file.fits"] == ["parts" + paths[0]]


def test_upload_cleanup_delivered(s3w, work_dir):
    # A key that was previously uploaded for a now-delivered file should be deleted from S3
    delivered_key = "parts/delivered_part"
    s3w._s3c.put_object(Bucket="test-bucket", Key=delivered_key, Body=b"data")
    s3w.put_as_json({}, "disassemble.json")
    s3w.put_as_json({"delivered.fits": [delivered_key]}, "upload.json")
    s3w.put_as_json({"delivered.fits": {"size": 1}}, "target.json")
    cta_ingest.upload(s3w, dry_run=False)
    assert "delivered.fits" not in s3w.get_from_json("upload.json", default={})
    assert delivered_key not in s3w.list_keys()


def test_upload_missing_target_raises(s3w, work_dir):
    paths = make_chunk_dir(work_dir, "file.fits")
    s3w.put_as_json({"file.fits": paths}, "disassemble.json")
    with pytest.raises(cta_ingest.NoSuchKeyError):
        cta_ingest.upload(s3w, dry_run=False)
