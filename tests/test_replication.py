import json
import os
import sys
from multiprocessing import Manager, Process
from time import time

import psycopg2
import pytest
from psycopg2.extras import LogicalReplicationConnection

from alluvium.db_utils import get_database_cursor
from alluvium.replication import (
    create_replication_slot,
    replication_slot_exists,
    ReplicationSerializationFormat,
    subscribe_to_replication_slot,
)

# Subscribe to test DB. Start a process which consumes a stream. Then writer will
TEST_FILE_NAME = "logical_replications.json"


@pytest.fixture
def slot_name():
    return "test_slot"


@pytest.fixture
def replication_cursor(postgres_pg_config):
    logical_replication_slot_config_options = postgres_pg_config.copy()
    logical_replication_slot_config_options.update(
        {"connection_factory": LogicalReplicationConnection}
    )
    with get_database_cursor(logical_replication_slot_config_options) as cursor:
        yield cursor


@pytest.fixture
def slot_teardown(replication_cursor, slot_name):
    yield
    if replication_slot_exists(slot_name, replication_cursor):
        replication_cursor.drop_replication_slot(slot_name)


def test_replication_slot_exists_not_exists(replication_cursor, slot_name):
    slot_exists = replication_slot_exists(slot_name, replication_cursor)
    assert not slot_exists


def test_replication_slot_exists_exists(replication_cursor, slot_name, slot_teardown):
    replication_cursor.create_replication_slot(
        slot_name, output_plugin=ReplicationSerializationFormat.WAL2JSON.value
    )
    slot_exists = replication_slot_exists(slot_name, replication_cursor)
    assert slot_exists


def test_create_replication_slot(replication_cursor, slot_name, slot_teardown):
    assert not replication_slot_exists(slot_name, replication_cursor)
    create_replication_slot(
        slot_name, replication_cursor, ReplicationSerializationFormat.WAL2JSON.value
    )
    assert replication_slot_exists(slot_name, replication_cursor)

    create_replication_slot(
        slot_name, replication_cursor, ReplicationSerializationFormat.WAL2JSON.value
    )
    assert replication_slot_exists(slot_name, replication_cursor)
