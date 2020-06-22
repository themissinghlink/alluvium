import logging

import click
from psycopg2.extras import LogicalReplicationConnection

from alluvium.consumer import (
    KinesisFirehoseConsumer,
    LogConsumer,
    Wal2JsonProcessorV1,
    Wal2JsonProcessorV2,
)
from alluvium.db_utils import get_database_cursor
from alluvium.replication import subscribe_to_replication_slot

PG_CONFIG = {
    "user": "test",
    "password": "test",
    "host": "localhost",
    "port": 5432,
    "database": "postgres",
    "connection_factory": LogicalReplicationConnection,
}


FORMAT_VERSION_ARG_TO_PROCESSOR = {"V1": (Wal2JsonProcessorV1, 1), "V2": (Wal2JsonProcessorV2, 2)}


logging.basicConfig(level=logging.DEBUG)


@click.group()
def cli():
    pass


@cli.command(name="start_logger_listener")
@click.option(
    "--format_version",
    "-f",
    type=click.Choice(["V1", "V2"], case_sensitive=False),
    help="The processor class which ",
)
def start_stdout_listener(format_version):
    processor, fmt_version = FORMAT_VERSION_ARG_TO_PROCESSOR[format_version]
    logging.info("Starting to listen:")
    with get_database_cursor(PG_CONFIG) as cursor:
        subscribe_to_replication_slot(
            cursor,
            "alluvium_stdout",
            LogConsumer(cdc_event_processor=processor),
            {"pretty-print": 1, "include-schemas": 1, "format-version": fmt_version},
            True,
        )


if __name__ == "__main__":
    cli()  # pylint:disable=no-value-for-parameter
