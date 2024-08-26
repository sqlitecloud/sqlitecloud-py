import os
import sqlite3
from copy import deepcopy

import pytest
from dotenv import load_dotenv

import sqlitecloud
from sqlitecloud.client import SQLiteCloudClient
from sqlitecloud.datatypes import SQLiteCloudAccount, SQLiteCloudConnect


@pytest.fixture(autouse=True)
def load_env_vars():
    load_dotenv(".env")


@pytest.fixture(autouse=True)
def reset_module_state():
    original_sqlc_adapters = deepcopy(sqlitecloud.adapters)
    original_sqlc_converters = deepcopy(sqlitecloud.converters)

    original_sql_adapters = deepcopy(sqlite3.adapters)
    original_sql_converters = deepcopy(sqlite3.converters)

    yield

    sqlitecloud.adapters.clear()
    sqlitecloud.converters.clear()

    sqlite3.adapters.clear()
    sqlite3.converters.clear()

    sqlitecloud.adapters.update(original_sqlc_adapters)
    sqlitecloud.converters.update(original_sqlc_converters)

    sqlite3.adapters.update(original_sql_adapters)
    sqlite3.converters.update(original_sql_converters)


@pytest.fixture()
def sqlitecloud_connection():
    account = SQLiteCloudAccount()
    account.username = os.getenv("SQLITE_USER")
    account.password = os.getenv("SQLITE_PASSWORD")
    account.dbname = os.getenv("SQLITE_DB")
    account.hostname = os.getenv("SQLITE_HOST")
    account.port = int(os.getenv("SQLITE_PORT"))

    client = SQLiteCloudClient(cloud_account=account)

    connection = client.open_connection()
    assert isinstance(connection, SQLiteCloudConnect)
    assert client.is_connected(connection)

    yield (connection, client)

    client.disconnect(connection)


@pytest.fixture()
def sqlitecloud_dbapi2_connection():
    # fixture and declaration are split to be able
    # to create multiple instances of the connection
    # when calling the getter function directly from
    # the test.
    # Fixtures are both cached and cannot be called
    # directly whithin the test.
    connection_generator = get_sqlitecloud_dbapi2_connection()

    connection = next(connection_generator)

    yield connection

    close_generator(connection_generator)


def get_sqlitecloud_dbapi2_connection(detect_types: int = 0):
    account = SQLiteCloudAccount()
    account.username = os.getenv("SQLITE_USER")
    account.password = os.getenv("SQLITE_PASSWORD")
    account.dbname = os.getenv("SQLITE_DB")
    account.hostname = os.getenv("SQLITE_HOST")
    account.port = int(os.getenv("SQLITE_PORT"))

    connection = sqlitecloud.connect(account, detect_types=detect_types)

    assert isinstance(connection, sqlitecloud.Connection)

    yield connection

    connection.close()


@pytest.fixture()
def sqlite3_connection():
    connection_generator = get_sqlite3_connection()

    connection = next(connection_generator)

    yield connection

    close_generator(connection_generator)


def get_sqlite3_connection(detect_types: int = 0):
    # set isolation_level=None to enable autocommit
    # and to be aligned with the behavior of SQLite Cloud
    connection = sqlite3.connect(
        os.path.join(os.path.dirname(__file__), "./assets/chinook.sqlite"),
        isolation_level=None,
        detect_types=detect_types,
    )
    yield connection

    connection.close()


def close_generator(generator):
    try:
        next(generator)
    except StopIteration:
        pass
