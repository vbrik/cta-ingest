import os
from pathlib import Path

import cta_ingest
import pytest


def make_chunk_dir(
    work_dir: Path, filename: str, num_chunks: int = 2, chunk_size: int = 1000
):
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
    assert s3w.list_object_keys(prefix="parts") == []
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
    assert set(s3w.list_object_keys(prefix="parts")) == set(state["file.fits"])


def test_upload_key_naming(s3w, work_dir):
    # Key is literally 'parts' + absolute_path_string
    paths = make_chunk_dir(work_dir, "file.fits", num_chunks=1)
    s3w.put_as_json({"file.fits": paths}, "disassemble.json")
    s3w.put_as_json({}, "target.json")
    cta_ingest.upload(s3w, dry_run=False)
    state = s3w.get_from_json("upload.json")
    assert state["file.fits"] == ["parts" + paths[0]]


# Source bug note: upload()'s cleanup loop in cta-ingest.py never appends to its
# `stats` list, so its "cleaned-up: N files" log line never fires. This test does
# not cover it because these tests do not inspect log output; the check below only
# validates the side effects (state cleared, S3 object deleted).
def test_upload_cleanup_delivered(s3w, work_dir):
    # A key that was previously uploaded for a now-delivered file should be deleted from S3
    delivered_key = "parts/delivered_part"
    s3w._s3c.put_object(Bucket="test-bucket", Key=delivered_key, Body=b"data")
    s3w.put_as_json({}, "disassemble.json")
    s3w.put_as_json({"delivered.fits": [delivered_key]}, "upload.json")
    s3w.put_as_json({"delivered.fits": {"size": 1}}, "target.json")
    cta_ingest.upload(s3w, dry_run=False)
    assert "delivered.fits" not in s3w.get_from_json("upload.json", default={})
    assert delivered_key not in s3w.list_object_keys()


def test_upload_resume_partial(s3w, work_dir):
    """Parts already present in S3 are not re-uploaded; missing parts are uploaded and recorded."""
    paths = make_chunk_dir(work_dir, "file.fits", num_chunks=3)
    first_key = "parts" + paths[0]
    # Pre-seed S3 with a recognizable body so we can verify it's left untouched.
    s3w._s3c.put_object(Bucket="test-bucket", Key=first_key, Body=b"preexisting-body")

    s3w.put_as_json({"file.fits": paths}, "disassemble.json")
    s3w.put_as_json({"file.fits": [first_key]}, "upload.json")
    s3w.put_as_json({"file.fits": {"path": paths[0], "size": 3000}}, "origin.json")
    s3w.put_as_json({}, "target.json")

    cta_ingest.upload(s3w, dry_run=False)

    state = s3w.get_from_json("upload.json")
    expected_keys = {"parts" + p for p in paths}
    assert set(state["file.fits"]) == expected_keys
    body = s3w._s3c.get_object(Bucket="test-bucket", Key=first_key)["Body"].read()
    assert body == b"preexisting-body"


def test_upload_cleanup_orphan(s3w, work_dir):
    # A file in upload.json but absent from origin.json is an orphan; its S3
    # objects should be deleted and the entry removed from state.
    orphan_key = "parts/orphan_part"
    s3w._s3c.put_object(Bucket="test-bucket", Key=orphan_key, Body=b"data")
    s3w.put_as_json({}, "disassemble.json")
    s3w.put_as_json({"orphan.fits": [orphan_key]}, "upload.json")
    s3w.put_as_json({}, "origin.json")
    s3w.put_as_json({}, "target.json")
    cta_ingest.upload(s3w, dry_run=False)
    assert "orphan.fits" not in s3w.get_from_json("upload.json", default={})
    assert orphan_key not in s3w.list_object_keys()


def test_upload_rogue_object_added_to_state(s3w, work_dir):
    """An object already in S3 but absent from upload.json is treated as rogue:
    it must be added to state (so it can be cleaned up later) and not re-uploaded."""
    paths = make_chunk_dir(work_dir, "file.fits", num_chunks=2)
    rogue_key = "parts" + paths[0]
    s3w._s3c.put_object(Bucket="test-bucket", Key=rogue_key, Body=b"rogue-body")

    s3w.put_as_json({"file.fits": paths}, "disassemble.json")
    s3w.put_as_json({}, "upload.json")  # rogue_key intentionally absent
    s3w.put_as_json({}, "target.json")

    cta_ingest.upload(s3w, dry_run=False)

    state = s3w.get_from_json("upload.json")
    assert rogue_key in state["file.fits"]
    # rogue object must not have been overwritten
    body = s3w._s3c.get_object(Bucket="test-bucket", Key=rogue_key)["Body"].read()
    assert body == b"rogue-body"


def test_upload_missing_target_raises(s3w, work_dir):
    paths = make_chunk_dir(work_dir, "file.fits")
    s3w.put_as_json({"file.fits": paths}, "disassemble.json")
    with pytest.raises(cta_ingest.NoSuchKeyError):
        cta_ingest.upload(s3w, dry_run=False)
