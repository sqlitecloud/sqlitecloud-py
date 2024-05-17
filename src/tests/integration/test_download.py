import os
import sqlite3
import tempfile

import pytest

from sqlitecloud import download
from sqlitecloud.client import SqliteCloudClient
from sqlitecloud.types import SQCloudConnect, SqliteCloudAccount


class TestDownload:
    @pytest.fixture()
    def sqlitecloud_connection(self):
        account = SqliteCloudAccount()
        account.username = os.getenv("SQLITE_USER")
        account.password = os.getenv("SQLITE_PASSWORD")
        account.dbname = os.getenv("SQLITE_DB")
        account.hostname = os.getenv("SQLITE_HOST")
        account.port = 8860

        client = SqliteCloudClient(cloud_account=account)

        connection = client.open_connection()
        assert isinstance(connection, SQCloudConnect)

        yield (connection, client)

        client.disconnect(connection)

    def test_download_database(self, sqlitecloud_connection):
        connection, _ = sqlitecloud_connection

        temp_file = tempfile.mkstemp(prefix="chinook")[1]
        download.download_db(connection, "chinook.sqlite", temp_file)

        db = sqlite3.connect(temp_file)
        cursor = db.execute("SELECT * FROM albums")

        assert cursor.description[0][0] == "AlbumId"
        assert cursor.description[1][0] == "Title"
