import os
from datetime import datetime
from json import dumps, JSONDecodeError, JSONDecoder, loads
from tempfile import TemporaryFile
from typing import Any, Dict, List

import boto3


"""
Convenience wrapper for common operations on AWS.
"""


def decode_non_delimited_json(content_unicode_str):
    decoder = JSONDecoder()
    decode_index, content_length = 0, len(content_unicode_str)

    objects = []
    while decode_index < content_length:
        try:
            obj, decode_index = decoder.raw_decode(content_unicode_str, decode_index)
            objects.append(obj)
        except JSONDecodeError:
            # Scan forward and keep trying to decode
            decode_index += 1
    return objects


def decode_new_line_delimited_json(content_fp):
    return [loads(line) for line in content_fp]


class S3Bucket(object):
    def __init__(self, bucket_name: str):
        self.bucket_name = bucket_name
        self.client = boto3.resource("s3")
        self.bucket = self.client.Bucket(bucket_name)

    def get_keys_with_prefix(self, prefix: str):
        """
        Returns a generator which contains a list of keys.
        """
        return self.bucket.objects.filter(Prefix=prefix)

    def get_nondelimited_json_objects_from_key(self, key: str) -> Dict[Any, Any]:
        outputs = []
        with TemporaryFile(mode="w+b") as fp:
            self.bucket.download_fileobj(key, fp)
            fp.seek(0)
            outputs = decode_non_delimited_json(fp.read().decode("utf8").replace("'", '"'))
        return outputs

    def get_new_line_delimited_json_objects_from_key(self, key: str) -> Dict[Any, Any]:
        outputs = []
        with TemporaryFile(mode="w+b") as fp:
            self.bucket.download_fileobj(key, fp)
            fp.seek(0)
            outputs = decode_new_line_delimited_json(fp)
        return outputs

    def upload_file_to_key(self, path_to_upload_file: str, key: str):
        if not os.path.exists(path_to_upload_file):
            raise ValueError(
                "Can't upload to {}. Filepath {} doesn't exist.".format(key, path_to_upload_file)
            )
        key = self.client.Object(self.bucket_name, key)
        with open(path_to_upload_file, "rb") as fp:
            key.put(Body=fp)
