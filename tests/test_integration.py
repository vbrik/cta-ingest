"""
End-to-end integration test: simulates a complete file transfer through all pipeline stages.

    refresh_origin → disassemble → upload → download → reassemble → refresh_target

Uses moto for S3 (no real network), real zstd/pzstd/split commands.
Kept in a separate file so it can be run selectively with:
    pytest tests/test_integration.py
"""
import os
import cta_ingest

PART_SIZE = 50_000  # 50 KB — produces multiple split chunks from 200 KB+ random data


def _run_all_stages(s3w, origin_dir, disassemble_work, download_work, reassemble_work, dst_dir):
    """Execute all six pipeline stages in order against a pre-populated origin_dir."""
    # target.json must exist before disassemble/upload/download (no default in source)
    s3w.put_as_json({}, "target.json")

    cta_ingest.refresh_terminus(s3w, origin_dir, excludes=["."], my_state_key="origin.json")
    cta_ingest.disassemble(s3w, disassemble_work, PART_SIZE, dry_run=False)
    cta_ingest.upload(s3w, dry_run=False)
    cta_ingest.download(s3w, download_work, dry_run=False)
    cta_ingest.reassemble(s3w, reassemble_work, dst_dir, dry_run=False)
    cta_ingest.refresh_terminus(s3w, dst_dir, excludes=["."], my_state_key="target.json")


def test_full_pipeline(s3w, tmp_path):
    origin_dir = tmp_path / "origin"
    origin_dir.mkdir()
    files = {
        "scan_001.fits": os.urandom(200_000),
        "scan_002.fits": os.urandom(200_000),
    }
    for name, data in files.items():
        (origin_dir / name).write_bytes(data)

    dst_dir = tmp_path / "dst"
    _run_all_stages(
        s3w,
        origin_dir,
        disassemble_work=tmp_path / "disassemble_work",
        download_work=tmp_path / "download_work",
        reassemble_work=tmp_path / "reassemble_work",
        dst_dir=dst_dir,
    )

    for name, content in files.items():
        output = dst_dir / name
        assert output.exists(), f"{name} not found in dst_dir"
        assert output.read_bytes() == content, f"content mismatch for {name}"

    target_state = s3w.get_from_json("target.json")
    assert set(target_state.keys()) == set(files.keys())
