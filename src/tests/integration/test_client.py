import os

import pytest
from sqlitecloud.client import SqliteCloudClient
from sqlitecloud.types import SQCloudConnect, SQCloudException, SqliteCloudAccount


class TestClient:
    @pytest.fixture
    def sqlitecloud_connection(self):
        account = SqliteCloudAccount()
        account.username = os.getenv("SQLITE_USER")
        account.password = os.getenv("SQLITE_PASSWORD")
        account.database = os.getenv("SQLITE_DB")
        account.hostname = os.getenv("SQLITE_HOST")
        account.port = 8860

        client = SqliteCloudClient(cloud_account=account)
        connection = client.open_connection()
        assert isinstance(connection, SQCloudConnect)

        yield connection

        client.disconnect(connection)

    def test_connection_with_credentials(self):
        account = SqliteCloudAccount()
        account.username = os.getenv("SQLITE_USER")
        account.password = os.getenv("SQLITE_PASSWORD")
        account.database = os.getenv("SQLITE_DB")
        account.hostname = os.getenv("SQLITE_HOST")
        account.port = 8860

        client = SqliteCloudClient(cloud_account=account)
        conn = client.open_connection()
        assert isinstance(conn, SQCloudConnect)

        client.disconnect(conn)

    def test_connection_with_apikey(self):
        account = SqliteCloudAccount()
        account.username = os.getenv("SQLITE_API_KEY")
        account.hostname = os.getenv("SQLITE_HOST")
        account.port = 8860

        client = SqliteCloudClient(cloud_account=account)
        conn = client.open_connection()
        assert isinstance(conn, SQCloudConnect)

        client.disconnect(conn)

    def test_connection_without_credentials_and_apikey(self):
        #pytest.raises(SQCloudException)

        account = SqliteCloudAccount()
        account.database = os.getenv("SQLITE_DB")
        account.hostname = os.getenv("SQLITE_HOST")
        account.port = 8860
        
        client = SqliteCloudClient(cloud_account=account)
        
        client.open_connection()

