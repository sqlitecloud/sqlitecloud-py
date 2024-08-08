import os

import pytest
from dotenv import load_dotenv

import sqlitecloud
from sqlitecloud.client import SQLiteCloudClient
from sqlitecloud.datatypes import SQLiteCloudAccount, SQLiteCloudConnect


@pytest.fixture(autouse=True)
def load_env_vars():
    load_dotenv(".env")


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
    yield next(get_sqlitecloud_dbapi2_connection())


def get_sqlitecloud_dbapi2_connection():
    account = SQLiteCloudAccount()
    account.username = os.getenv("SQLITE_USER")
    account.password = os.getenv("SQLITE_PASSWORD")
    account.dbname = os.getenv("SQLITE_DB")
    account.hostname = os.getenv("SQLITE_HOST")
    account.port = int(os.getenv("SQLITE_PORT"))

    connection = sqlitecloud.connect(account)

    assert isinstance(connection, sqlitecloud.Connection)

    yield connection

    connection.close()
