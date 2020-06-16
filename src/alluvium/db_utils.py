from contextlib import contextmanager
from typing import Any, Dict

from psycopg2 import connect


@contextmanager
def get_database_cursor(connection_options: Dict[str, Any], autocommit: bool = False):
    connection = connect(**connection_options)
    if hasattr(connection, "autocommit"):
        connection.autocommit = autocommit
    with connection.cursor() as cursor:
        yield cursor
    connection.close()
