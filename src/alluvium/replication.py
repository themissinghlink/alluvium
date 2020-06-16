from contextlib import contextmanager
from enum import Enum
from typing import Any, Dict

from psycopg2 import connect
from psycopg2.extensions import cursor

from alluvium.consumer import Consumer
from alluvium.db_utils import get_database_cursor


class ReplicationSerializationFormat(Enum):
    WAL2JSON = "wal2json"


def replication_slot_exists(slot_name: str, replication_slot_cursor: cursor) -> bool:
    command = "select count(*) from pg_replication_slots where slot_name='{slot_name}'".format(
        slot_name=slot_name
    )
    replication_slot_cursor.execute(command)
    count = replication_slot_cursor.fetchone()[0]
    return count != 0


def create_replication_slot(slot_name: str, replication_slot_cursor: cursor, output_format: str):
    if replication_slot_exists(slot_name, replication_slot_cursor):
        replication_slot_cursor.drop_replication_slot(slot_name)
    replication_slot_cursor.create_replication_slot(slot_name, output_plugin=output_format)


def start_replication(
    slot_name: str,
    replication_slot_cursor: cursor,
    replication_options: Dict[str, Any],
    decode: bool = True,
):
    create_replication_slot(
        slot_name, replication_slot_cursor, ReplicationSerializationFormat.WAL2JSON.value
    )
    replication_slot_cursor.start_replication(
        slot_name=slot_name, options=replication_options, decode=decode
    )


def subscribe_to_replication_slot(
    cursor: cursor,
    slot_name: str,
    consumer: Consumer,
    replication_options: Dict[str, Any],
    decode: bool = True,
):
    start_replication(slot_name, cursor, replication_options, decode)
    cursor.consume_stream(consumer)
