import pytest

from moto import mock_s3


@mock_s3
def test_get_keys_with_prefix(fake_bucket):
    pass
