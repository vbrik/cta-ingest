import cta_ingest
import pytest


def test_put_get_json_roundtrip(s3w):
    data = {"key": [1, 2, 3], "nested": {"a": True, "b": None}}
    s3w.put_as_json(data, "roundtrip.json")
    assert s3w.get_from_json("roundtrip.json") == data


def test_put_json_overwrites(s3w):
    s3w.put_as_json({"v": 1}, "k.json")
    s3w.put_as_json({"v": 2}, "k.json")
    assert s3w.get_from_json("k.json") == {"v": 2}


def test_get_json_missing_with_default(s3w):
    assert s3w.get_from_json("nonexistent.json", default={"x": 2}) == {"x": 2}


# get_from_json collapses every botocore.ClientError into NoSuchKeyError, so a
# non-NoSuchKey failure (e.g. AccessDenied) is surfaced under a misleading name.
# Not covered here because moto does not make it straightforward to simulate
# such errors against a mocked S3.
def test_get_json_missing_no_default_raises(s3w):
    with pytest.raises(cta_ingest.NoSuchKeyError):
        s3w.get_from_json("nonexistent.json")


def test_upload_download_file_roundtrip(s3w, tmp_path):
    src = tmp_path / "upload_src.bin"
    src.write_bytes(b"\x00\x01\x02\xff" * 100)
    s3w.upload_file(str(src), "mykey/file.bin")
    dst = tmp_path / "download_dst.bin"
    s3w.download_file("mykey/file.bin", str(dst))
    assert dst.read_bytes() == src.read_bytes()


def test_delete_removes_key(s3w):
    s3w.put_as_json({}, "todelete.json")
    s3w.delete_object("todelete.json")
    assert s3w.get_from_json("todelete.json", default="MISSING") == "MISSING"


def test_delete_nonexistent_is_silent(s3w):
    s3w.delete_object("does-not-exist.json")


def test_list_keys_empty(s3w):
    assert s3w.list_object_keys() == []


def test_list_keys_all(s3w):
    s3w.put_as_json({}, "a.json")
    s3w.put_as_json({}, "b.json")
    assert sorted(s3w.list_object_keys()) == ["a.json", "b.json"]


def test_list_keys_with_prefix(s3w):
    s3w.put_as_json({}, "parts/x/aa")
    s3w.put_as_json({}, "parts/x/ab")
    s3w.put_as_json({}, "other/z")
    assert sorted(s3w.list_object_keys(prefix="parts")) == ["parts/x/aa", "parts/x/ab"]
