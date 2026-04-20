import importlib.util
import os
import sys
from pathlib import Path

import pytest
from moto import mock_aws

_spec = importlib.util.spec_from_file_location(
    "cta_ingest", Path(__file__).parent.parent / "cta-ingest.py"
)
cta_ingest = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(cta_ingest)
sys.modules["cta_ingest"] = cta_ingest


@pytest.fixture(autouse=True, scope="session")
def aws_credentials():
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_SECURITY_TOKEN"] = "test"
    os.environ["AWS_SESSION_TOKEN"] = "test"


@pytest.fixture
def s3_mock():
    with mock_aws():
        yield


@pytest.fixture
def s3w(s3_mock):
    return cta_ingest.S3Wrapper(endpoint_url=None, bucket="test-bucket")


@pytest.fixture
def work_dir(tmp_path):
    d = tmp_path / "work"
    d.mkdir()
    return d


@pytest.fixture
def origin_dir(tmp_path):
    d = tmp_path / "origin"
    d.mkdir()
    return d
