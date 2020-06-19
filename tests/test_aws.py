import json
import os
from tempfile import TemporaryFile

import boto3
import pytest
from moto import mock_s3


@pytest.fixture
def bucket_name():
    return "fake_bucket"


@pytest.fixture
def mock_bucket(bucket_name):
    with mock_s3():
        client = boto3.resource("s3")
        client.create_bucket(Bucket=bucket_name)
        fake_bucket = client.Bucket(bucket_name)

        for val in ["bar", "baz", "qux"]:
            fake_file_contents = {"foo": val}
            file_name = "{}.json".format(val)
            key = client.Object(bucket_name, file_name)
            with open(file_name, "w+") as fp:
                json.dump(fake_file_contents, fp)
            with open(file_name, "r+b") as fp:
                key.put(Body=fp)
            os.remove(file_name)
        yield fake_bucket


def test_get_keys_with_prefix(mock_bucket):
    assert [o.key for o in mock_bucket.objects.all()] == ["bar.json", "baz.json", "qux.json"]
