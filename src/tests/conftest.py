import os

import pytest
from dotenv import load_dotenv

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
    account.port = 8860

    client = SqliteCloudClient(cloud_account=account)

    connection = client.open_connection()
    assert isinstance(connection, SQCloudConnect)
    assert client.is_connected(connection)

    yield (connection, client)

    client.disconnect(connection)
