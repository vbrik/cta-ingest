import os
import pytest
from pathlib import Path
import cta_ingest


def upload_fake_parts(s3w, filename: str, num_parts: int = 2, part_size: int = 500):
    """Upload random part data directly to S3. Returns (upload_state, {key: bytes})."""
    keys = []
    content_map = {}
    for i in range(num_parts):
        key = f"parts/work/{filename}/chunk_{i:02d}"
        data = os.urandom(part_size)
        s3w._s3c.put_object(Bucket="test-bucket", Key=key, Body=data)
        keys.append(key)
        content_map[key] = data
    return {filename: keys}, content_map


def test_download_dry_run(s3w, work_dir):
    upload_state, _ = upload_fake_parts(s3w, "file.fits")
    s3w.put_as_json(upload_state, "upload.json")
    s3w.put_as_json({}, "target.json")
    cta_ingest.download(s3w, work_dir, dry_run=True)
    assert not any(work_dir.iterdir())
    assert s3w.get_from_json("download.json", default={}) == {}


def test_download_downloads_parts(s3w, work_dir):
    upload_state, content_map = upload_fake_parts(s3w, "file.fits", num_parts=3)
    s3w.put_as_json(upload_state, "upload.json")
    s3w.put_as_json({}, "target.json")
    cta_ingest.download(s3w, work_dir, dry_run=False)
    state = s3w.get_from_json("download.json")
    assert "file.fits" in state
    part_paths = state["file.fits"]
    assert len(part_paths) == 3
    assert all(Path(p).exists() for p in part_paths)
    for key, expected in content_map.items():
        matching = [p for p in part_paths if Path(p).name == Path(key).name]
        assert len(matching) == 1
        assert Path(matching[0]).read_bytes() == expected


def test_download_skips_target_files(s3w, work_dir):
    upload_state, _ = upload_fake_parts(s3w, "done.fits")
    s3w.put_as_json(upload_state, "upload.json")
    s3w.put_as_json({"done.fits": {"size": 1}}, "target.json")
    cta_ingest.download(s3w, work_dir, dry_run=False)
    assert "done.fits" not in s3w.get_from_json("download.json", default={})


def test_download_cleanup_delivered(s3w, work_dir):
    delivered = "delivered.fits"
    chunk_dir = work_dir / delivered
    chunk_dir.mkdir()
    (chunk_dir / "aa").write_bytes(b"x")
    s3w.put_as_json({delivered: [str(chunk_dir / "aa")]}, "download.json")
    s3w.put_as_json({}, "upload.json")
    s3w.put_as_json({delivered: {"size": 1}}, "target.json")
    cta_ingest.download(s3w, work_dir, dry_run=False)
    assert not chunk_dir.exists()
    assert delivered not in s3w.get_from_json("download.json", default={})


def test_download_idempotent(s3w, work_dir):
    # A part already listed in download.json must not be overwritten
    upload_state, content_map = upload_fake_parts(s3w, "file.fits", num_parts=2)
    chunk_dir = work_dir / "file.fits"
    chunk_dir.mkdir()
    first_key = sorted(content_map.keys())[0]
    first_path = str(chunk_dir / Path(first_key).name)
    Path(first_path).write_bytes(b"preexisting")
    s3w.put_as_json({"file.fits": [first_path]}, "download.json")
    s3w.put_as_json(upload_state, "upload.json")
    s3w.put_as_json({}, "target.json")
    cta_ingest.download(s3w, work_dir, dry_run=False)
    assert Path(first_path).read_bytes() == b"preexisting"


def test_download_missing_target_raises(s3w, work_dir):
    s3w.put_as_json({"f.fits": []}, "upload.json")
    with pytest.raises(cta_ingest.NoSuchKeyError):
        cta_ingest.download(s3w, work_dir, dry_run=False)
