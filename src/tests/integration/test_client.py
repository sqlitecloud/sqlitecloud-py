from operator import rshift
import os
from typing import Union

import pytest
from sqlitecloud.client import SqliteCloudClient
from sqlitecloud.types import SQCloudConnect, SQCloudException, SqliteCloudAccount


class TestClient:
    @pytest.fixture()
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

        yield (connection, client)

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
        account = SqliteCloudAccount()
        account.database = os.getenv("SQLITE_DB")
        account.hostname = os.getenv("SQLITE_HOST")
        account.port = 8860

        client = SqliteCloudClient(cloud_account=account)

        with pytest.raises(SQCloudException):
            client.open_connection()

    def test_connect_with_string(self):
        connection_string = os.getenv("SQLITE_CONNECTION_STRING")

        client = SqliteCloudClient(connection_str=connection_string)

        conn = client.open_connection()
        assert isinstance(conn, SQCloudConnect)

        client.disconnect(conn)

    def test_connect_with_string_with_credentials(self):
        connection_string = f"sqlitecloud://{os.getenv('SQLITE_USER')}:{os.getenv('SQLITE_PASSWORD')}@{os.getenv('SQLITE_HOST')}/{os.getenv('SQLITE_DB')}"

        client = SqliteCloudClient(connection_str=connection_string)

        conn = client.open_connection()
        assert isinstance(conn, SQCloudConnect)

        client.disconnect(conn)

    def test_select(self, sqlitecloud_connection):
        connection, client = sqlitecloud_connection

        result = client.exec_query("SELECT 'Hello'", connection)

        assert result
        assert False == result.is_result
        assert 1 == result.nrows
        assert 1 == result.ncols
        assert "Hello" == result.get_value(0, 0)

    def test_rowset_data(self, sqlitecloud_connection):
        connection, client = sqlitecloud_connection
        result = client.exec_query("SELECT AlbumId FROM albums LIMIT 2", connection)
        assert result
        assert 2 == result.nrows
        assert 1 == result.ncols
        assert 2 == result.version

    def test_get_value(self, sqlitecloud_connection):
        connection, client = sqlitecloud_connection
        result = client.exec_query("SELECT * FROM albums", connection)
        assert result
        assert "1" == result.get_value(0, 0)
        assert "For Those About To Rock We Salute You" == result.get_value(0, 1)
        assert "2" == result.get_value(1, 0)

    def test_get_utf8_value(self, sqlitecloud_connection):
        connection, client = sqlitecloud_connection
        result = client.exec_query("SELECT 'Minha História'", connection)
        assert result
        assert "Minha História" == result.get_value(0, 0)
