import pytest

from alluvium.db_utils import get_database_cursor

TEST_TABLE = "table_with_pk"


@pytest.fixture(scope="module")
def postgres_pg_config():
    return {
        "user": "test",
        "password": "test",
        "host": "localhost",
        "port": 5432,
        "database": "postgres",
    }


@pytest.fixture(scope="module")
def test_db_cursor(postgres_pg_config):
    with get_database_cursor(postgres_pg_config, autocommit=True) as cursor:
        try:
            cursor.execute(
                "create table if not exists {}(a serial, b varchar(30), primary key(a));".format(
                    TEST_TABLE
                )
            )
            yield cursor
        finally:
            cursor.execute("drop table if exists {};".format(TEST_TABLE))
