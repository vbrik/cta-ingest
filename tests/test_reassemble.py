import subprocess
import pytest
from pathlib import Path
import cta_ingest


def make_zstd_parts(setup_dir: Path, filename: str, content: bytes, num_parts: int = 3):
    """
    Compress content with zstd and split the compressed bytes into num_parts files.
    Parts are named so that lexicographic sort gives the correct reconstruction order.
    Returns (sorted_part_paths, origin_state_dict).
    """
    setup_dir.mkdir(parents=True, exist_ok=True)
    src = setup_dir / "src"
    src.write_bytes(content)
    compressed = setup_dir / "compressed.zst"
    subprocess.run(
        ["zstd", "--threads=0", "--stdout", str(src)],
        stdout=compressed.open("wb"),
        check=True,
    )
    comp_bytes = compressed.read_bytes()
    chunk_size = max(1, (len(comp_bytes) + num_parts - 1) // num_parts)
    parts_dir = setup_dir / filename
    parts_dir.mkdir()
    part_paths = []
    for i in range(num_parts):
        chunk = comp_bytes[i * chunk_size : (i + 1) * chunk_size]
        if not chunk:
            break
        p = parts_dir / f"part_{i:02d}"
        p.write_bytes(chunk)
        part_paths.append(str(p))
    stat = src.stat()
    origin_state = {
        filename: {
            "path": str(setup_dir / filename),
            "size": len(content),
            "mtime": stat.st_mtime,
            "atime": stat.st_atime,
        }
    }
    return sorted(part_paths), origin_state


def test_reassemble_basic(s3w, tmp_path):
    content = b"hello cta-ingest " * 10_000
    filename = "myfile.dat"
    work_dir = tmp_path / "work"
    dst_dir = tmp_path / "dst"
    part_paths, origin_state = make_zstd_parts(tmp_path / "setup", filename, content, num_parts=3)
    s3w.put_as_json({filename: part_paths}, "download.json")
    s3w.put_as_json(origin_state, "origin.json")
    cta_ingest.reassemble(s3w, work_dir, dst_dir, dry_run=False)
    output = dst_dir / filename
    assert output.exists()
    # Stat before reading: read_bytes() can advance atime on strictatime filesystems.
    stat = output.stat()
    assert oct(stat.st_mode & 0o777) == "0o444"
    assert abs(stat.st_mtime - origin_state[filename]["mtime"]) < 1.0
    assert abs(stat.st_atime - origin_state[filename]["atime"]) < 1.0
    assert output.read_bytes() == content


def test_reassemble_skips_empty_parts(s3w, tmp_path):
    s3w.put_as_json({"badfile.dat": []}, "download.json")
    s3w.put_as_json({"badfile.dat": {"mtime": 0.0, "atime": 0.0, "size": 0}}, "origin.json")
    work_dir = tmp_path / "work"
    dst_dir = tmp_path / "dst"
    cta_ingest.reassemble(s3w, work_dir, dst_dir, dry_run=False)  # must not raise
    # reassemble() creates dst_dir unconditionally, so only emptiness is meaningful here.
    assert not any(dst_dir.iterdir())


def test_reassemble_missing_origin_raises(s3w, tmp_path):
    s3w.put_as_json({"f.dat": ["/fake/path"]}, "download.json")
    with pytest.raises(cta_ingest.NoSuchKeyError):
        cta_ingest.reassemble(s3w, tmp_path / "work", tmp_path / "dst", dry_run=False)


def test_reassemble_dst_dir_created(s3w, tmp_path):
    content = b"create me " * 200
    filename = "f.dat"
    dst_dir = tmp_path / "nonexistent" / "subdir"
    part_paths, origin_state = make_zstd_parts(tmp_path / "setup", filename, content)
    s3w.put_as_json({filename: part_paths}, "download.json")
    s3w.put_as_json(origin_state, "origin.json")
    cta_ingest.reassemble(s3w, tmp_path / "work", dst_dir, dry_run=False)
    assert dst_dir.exists()
    assert (dst_dir / filename).exists()


def test_reassemble_skips_if_dst_exists(s3w, tmp_path):
    content = b"new content " * 200
    filename = "dup.dat"
    dst_dir = tmp_path / "dst"
    dst_dir.mkdir()
    (dst_dir / filename).write_bytes(b"existing")
    part_paths, origin_state = make_zstd_parts(tmp_path / "setup", filename, content)
    s3w.put_as_json({filename: part_paths}, "download.json")
    s3w.put_as_json(origin_state, "origin.json")
    cta_ingest.reassemble(s3w, tmp_path / "work", dst_dir, dry_run=False)
    assert (dst_dir / filename).read_bytes() == b"existing"


def test_reassemble_continues_after_failure(s3w, tmp_path):
    """A corrupted file's parts must not prevent other files from reassembling."""
    good_content = b"good payload " * 500
    good_parts, good_origin = make_zstd_parts(tmp_path / "good_setup", "good.dat", good_content)

    bad_parts_dir = tmp_path / "bad_setup" / "bad.dat"
    bad_parts_dir.mkdir(parents=True)
    bad_part = bad_parts_dir / "part_00"
    bad_part.write_bytes(b"this is not a valid zstd stream")
    bad_origin = {
        "bad.dat": {"path": str(bad_parts_dir), "size": 0, "mtime": 0.0, "atime": 0.0}
    }

    s3w.put_as_json(
        {"good.dat": good_parts, "bad.dat": [str(bad_part)]}, "download.json"
    )
    s3w.put_as_json({**good_origin, **bad_origin}, "origin.json")

    dst_dir = tmp_path / "dst"
    cta_ingest.reassemble(s3w, tmp_path / "work", dst_dir, dry_run=False)

    assert (dst_dir / "good.dat").exists()
    assert (dst_dir / "good.dat").read_bytes() == good_content
    assert not (dst_dir / "bad.dat").exists()


def test_reassemble_dry_run_skips_output(s3w, tmp_path):
    content = b"dry run payload " * 500
    filename = "dryfile.dat"
    work_dir = tmp_path / "work"
    dst_dir = tmp_path / "dst"
    part_paths, origin_state = make_zstd_parts(tmp_path / "setup", filename, content)
    s3w.put_as_json({filename: part_paths}, "download.json")
    s3w.put_as_json(origin_state, "origin.json")
    cta_ingest.reassemble(s3w, work_dir, dst_dir, dry_run=True)
    assert not (dst_dir / filename).exists()
