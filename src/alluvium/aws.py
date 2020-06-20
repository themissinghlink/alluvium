import os
from datetime import datetime
from json import dumps, JSONDecodeError, JSONDecoder, load, loads
from tempfile import NamedTemporaryFile, TemporaryFile
from typing import Any, Dict, List

import boto3

from alluvium.serdes import decode_new_line_delimited_json, decode_non_delimited_json


"""
Convenience wrapper for common operations on AWS.
"""


def get_s3_bucket(bucket_name: str):
    return boto3.resource("s3").Bucket(bucket_name)


def create_s3_object(bucket_name: str, key: str):
    return boto3.resource("s3").Object(bucket_name, key)


def get_s3_keys_with_prefix(bucket_name: str, prefix: str):
    s3_bucket = get_s3_bucket(bucket_name)
    return s3_bucket.objects.filter(Prefix=prefix)


def get_json_from_key(bucket_name: str, key: str) -> Dict[Any, Any]:
    s3_bucket = get_s3_bucket(bucket_name)
    with NamedTemporaryFile(mode="w+b") as fp:
        s3_bucket.download_fileobj(key, fp)
        fp.seek(0)
        with open(fp.name, "r") as inner_fp:
            outputs = load(inner_fp)
    return outputs


def get_nondelimited_json_objects_from_key(bucket_name: str, key: str) -> Dict[Any, Any]:
    s3_bucket = get_s3_bucket(bucket_name)
    outputs = []
    with TemporaryFile(mode="w+b") as fp:
        s3_bucket.download_fileobj(key, fp)
        fp.seek(0)
        outputs = decode_non_delimited_json(fp.read().decode("utf8").replace("'", '"'))
    return outputs


def get_new_line_delimited_json_objects_from_key(bucket_name: str, key: str) -> Dict[Any, Any]:
    s3_bucket = get_s3_bucket(bucket_name)
    outputs = []
    with TemporaryFile(mode="w+b") as fp:
        s3_bucket.download_fileobj(key, fp)
        fp.seek(0)
        outputs = decode_new_line_delimited_json(fp)
    return outputs


def upload_file_to_key(bucket_name: str, path_to_upload_file: str, key: str):
    if not os.path.exists(path_to_upload_file):
        raise OSError(
            "Can't upload to {}. Filepath {} doesn't exist.".format(key, path_to_upload_file)
        )
    key = create_s3_object(bucket_name, key)
    with open(path_to_upload_file, "rb") as fp:
        key.put(Body=fp)
