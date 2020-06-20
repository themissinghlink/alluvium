import json
import os
from tempfile import NamedTemporaryFile

import boto3
import pytest
from moto import mock_s3

from alluvium.aws import (
    get_json_from_key,
    get_new_line_delimited_json_objects_from_key,
    get_s3_keys_with_prefix,
    upload_file_to_key,
)


@pytest.fixture
def bucket_name():
    return "fake_bucket"


@pytest.fixture
def mock_bucket_keys():
    return ["bar.json", "baz.json", "qux.json"]


@pytest.fixture
def mock_bucket(bucket_name, mock_bucket_keys):
    with mock_s3():
        client = boto3.resource("s3")
        client.create_bucket(Bucket=bucket_name)
        fake_bucket = client.Bucket(bucket_name)
        keys = []

        for i, file_name in enumerate(mock_bucket_keys):
            key = client.Object(bucket_name, file_name)
            with NamedTemporaryFile("w+") as fp:
                json.dump({"foo": i}, fp)
                fp.seek(0)
                with open(fp.name, "rb") as inner_fp:
                    key.put(Body=inner_fp)
            keys.append(file_name)
        yield bucket_name


def test_get_keys_with_prefix_ok(mock_bucket, mock_bucket_keys):
    s3_objects = get_s3_keys_with_prefix(mock_bucket, "")
    assert [obj.key for obj in s3_objects] == mock_bucket_keys
    for i, key in enumerate(mock_bucket_keys):
        assert get_json_from_key(mock_bucket, key) == {"foo": i}


def test_get_keys_with_prefix_not_exists(mock_bucket, mock_bucket_keys):
    s3_objects = get_s3_keys_with_prefix(mock_bucket, "NOT_A_REAL_LOCATION")
    assert not list(s3_objects)


def test_upload_to_s3(mock_bucket):
    with NamedTemporaryFile("w+") as fp:
        json.dump({"foo": "bar"}, fp)
        fp.seek(0)
        upload_file_to_key(mock_bucket, fp.name, "cool.json")
    s3_objects = get_s3_keys_with_prefix(mock_bucket, "")
    assert "cool.json" in [obj.key for obj in s3_objects]
    assert get_json_from_key(mock_bucket, "cool.json") == {"foo": "bar"}


def test_upload_to_s3_not_exists_in_filesystem(mock_bucket):
    with pytest.raises(OSError):
        upload_file_to_key(mock_bucket, "NOT_A_REAL_FILE", "cool.json")


def test_get_new_line_delimited_json_objects_from_key_ok(mock_bucket):
    with NamedTemporaryFile("w+") as fp:
        fp.write(json.dumps({"foo": 0}) + "\n")
        fp.write(json.dumps({"foo": 1}) + "\n")
        fp.seek(0)
        upload_file_to_key(mock_bucket, fp.name, "cool.json")
    s3_objects = get_s3_keys_with_prefix(mock_bucket, "")
    assert "cool.json" in [obj.key for obj in s3_objects]
    assert get_new_line_delimited_json_objects_from_key(mock_bucket, "cool.json") == [
        {"foo": 0},
        {"foo": 1},
    ]
