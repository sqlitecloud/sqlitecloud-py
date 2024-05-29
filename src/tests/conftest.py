import os

import pytest
from dotenv import load_dotenv

import sqlitecloud
from sqlitecloud.client import SqliteCloudClient
from sqlitecloud.types import SQCloudConnect, SqliteCloudAccount


@pytest.fixture(autouse=True)
def load_env_vars():
    load_dotenv(".env")


@pytest.fixture()
def sqlitecloud_connection():
    account = SqliteCloudAccount()
    account.username = os.getenv("SQLITE_USER")
    account.password = os.getenv("SQLITE_PASSWORD")
    account.dbname = os.getenv("SQLITE_DB")
    account.hostname = os.getenv("SQLITE_HOST")
    account.port = int(os.getenv("SQLITE_PORT"))

    client = SqliteCloudClient(cloud_account=account)

    connection = client.open_connection()
    assert isinstance(connection, SQCloudConnect)
    assert client.is_connected(connection)

    yield (connection, client)

    client.disconnect(connection)


@pytest.fixture()
def sqlitecloud_dbapi2_connection():
    account = SqliteCloudAccount()
    account.username = os.getenv("SQLITE_USER")
    account.password = os.getenv("SQLITE_PASSWORD")
    account.dbname = os.getenv("SQLITE_DB")
    account.hostname = os.getenv("SQLITE_HOST")
    account.port = int(os.getenv("SQLITE_PORT"))

    connection = sqlitecloud.connect(account)

    assert isinstance(connection, sqlitecloud.Connection)

    yield connection

    connection.close()
