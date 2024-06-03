import os
import uuid

import pytest

import sqlitecloud
from sqlitecloud.types import (
    SQLITECLOUD_INTERNAL_ERRCODE,
    SQLiteCloudAccount,
    SQLiteCloudException,
)


class TestDBAPI2:
    def test_connect_with_account(self):
        account = SQLiteCloudAccount(
            os.getenv("SQLITE_USER"),
            os.getenv("SQLITE_PASSWORD"),
            os.getenv("SQLITE_HOST"),
            os.getenv("SQLITE_DB"),
            int(os.getenv("SQLITE_PORT")),
        )

        connection = sqlitecloud.connect(account)

        connection.close()
        assert isinstance(connection, sqlitecloud.Connection)

    def test_connect_with_connection_string(self):
        connection_str = f"{os.getenv('SQLITE_CONNECTION_STRING')}/{os.getenv('SQLITE_DB')}?apikey={os.getenv('SQLITE_API_KEY')}"

        connection = sqlitecloud.connect(connection_str)

        connection.close()
        assert isinstance(connection, sqlitecloud.Connection)

    def test_disconnect(self):
        account = SQLiteCloudAccount(
            os.getenv("SQLITE_USER"),
            os.getenv("SQLITE_PASSWORD"),
            os.getenv("SQLITE_HOST"),
            os.getenv("SQLITE_DB"),
            int(os.getenv("SQLITE_PORT")),
        )

        connection = sqlitecloud.connect(account)

        connection.close()

        assert isinstance(connection, sqlitecloud.Connection)

        with pytest.raises(SQLiteCloudException) as e:
            connection.execute("SELECT 1")

        assert e.value.errcode == SQLITECLOUD_INTERNAL_ERRCODE.NETWORK
        assert e.value.errmsg == "The connection is closed."

    def test_select(self, sqlitecloud_dbapi2_connection):
        connection = sqlitecloud_dbapi2_connection

        cursor = connection.cursor()
        cursor.execute("SELECT 'Hello'")

        result = cursor.fetchone()

        assert result == ("Hello",)

    def test_connection_execute(self, sqlitecloud_dbapi2_connection):
        connection = sqlitecloud_dbapi2_connection

        cursor = connection.execute("SELECT 'Hello'")

        result = cursor.fetchone()

        assert result == ("Hello",)

    def test_column_not_found(self, sqlitecloud_dbapi2_connection):
        connection = sqlitecloud_dbapi2_connection

        with pytest.raises(SQLiteCloudException) as e:
            connection.execute("SELECT not_a_column FROM albums")

        assert e.value.errcode == 1
        assert e.value.errmsg == "no such column: not_a_column"

    def test_rowset_data(self, sqlitecloud_dbapi2_connection):
        connection = sqlitecloud_dbapi2_connection
        cursor = connection.execute("SELECT AlbumId FROM albums LIMIT 2")

        assert cursor.rowcount == 2
        assert len(cursor.description) == 1

    def test_fetch_one_row(self, sqlitecloud_dbapi2_connection):
        connection = sqlitecloud_dbapi2_connection

        cursor = connection.execute("SELECT * FROM albums")

        row = cursor.fetchone()

        assert len(row) == 3
        assert row == (1, "For Those About To Rock We Salute You", 1)

    def test_select_utf8_value_and_column_name(self, sqlitecloud_dbapi2_connection):
        connection = sqlitecloud_dbapi2_connection
        cursor = connection.execute("SELECT 'Minha História'")

        assert cursor.rowcount == 1
        assert len(cursor.description) == 1
        assert "Minha História" == cursor.fetchone()[0]
        assert "'Minha História'" == cursor.description[0][0]

    def test_column_name(self, sqlitecloud_dbapi2_connection):
        connection = sqlitecloud_dbapi2_connection
        cursor = connection.execute("SELECT * FROM albums")

        assert "AlbumId" == cursor.description[0][0]
        assert "Title" == cursor.description[1][0]

    def test_integer(self, sqlitecloud_dbapi2_connection):
        connection = sqlitecloud_dbapi2_connection
        cursor = connection.execute("TEST INTEGER")

        assert cursor.rowcount == -1
        assert cursor.fetchone() is None

    def test_error(self, sqlitecloud_dbapi2_connection):
        connection = sqlitecloud_dbapi2_connection

        with pytest.raises(SQLiteCloudException) as e:
            connection.execute("TEST ERROR")

        assert e.value.errcode == 66666
        assert e.value.errmsg == "This is a test error message with a devil error code."

    def test_execute_with_named_placeholder(self, sqlitecloud_dbapi2_connection):
        connection = sqlitecloud_dbapi2_connection

        cursor = connection.execute(
            "SELECT * FROM albums WHERE AlbumId = :id and Title like :title",
            {"id": 1, "title": "For Those About%"},
        )

        assert cursor.rowcount == 1
        assert cursor.fetchone() == (1, "For Those About To Rock We Salute You", 1)

    def test_execute_with_qmarks(self, sqlitecloud_dbapi2_connection):
        connection = sqlitecloud_dbapi2_connection

        cursor = connection.execute(
            "SELECT * FROM albums WHERE AlbumId = ? and Title like ?",
            (
                1,
                "For Those About%",
            ),
        )

        assert cursor.rowcount == 1
        assert cursor.fetchone() == (1, "For Those About To Rock We Salute You", 1)

    def test_execute_two_updates_using_the_cursor(self, sqlitecloud_dbapi2_connection):
        connection = sqlitecloud_dbapi2_connection

        cursor = connection.cursor()
        new_name1 = "Jazz" + str(uuid.uuid4())
        new_name2 = "Jazz" + str(uuid.uuid4())
        genreId = 2

        cursor.execute(
            "UPDATE genres SET Name = ? WHERE GenreId = ?;",
            (
                new_name1,
                genreId,
            ),
        )
        cursor.execute(
            "UPDATE genres SET Name = ? WHERE GenreId = ?;",
            (
                new_name2,
                genreId,
            ),
        )

        cursor.execute(
            "SELECT Name, GenreID FROM genres WHERE GenreId = :id", {"id": genreId}
        )

        assert cursor.fetchone() == (
            new_name2,
            genreId,
        )

    def test_executemany_updates(self, sqlitecloud_dbapi2_connection):
        connection = sqlitecloud_dbapi2_connection

        new_name1 = "Jazz" + str(uuid.uuid4())
        new_name2 = "Jazz" + str(uuid.uuid4())
        genreId = 2

        cursor = connection.executemany(
            "UPDATE genres SET Name = ? WHERE GenreId = ?;",
            [(new_name1, genreId), (new_name2, genreId)],
        )

        cursor.execute(
            "SELECT Name, GenreID FROM genres WHERE GenreId = :id", {"id": genreId}
        )

        assert cursor.fetchone() == (
            new_name2,
            genreId,
        )

    def test_executemany_updates_using_the_cursor(self, sqlitecloud_dbapi2_connection):
        connection = sqlitecloud_dbapi2_connection

        cursor = connection.cursor()
        new_name1 = "Jazz" + str(uuid.uuid4())
        new_name2 = "Jazz" + str(uuid.uuid4())
        genreId = 2

        cursor.executemany(
            "UPDATE genres SET Name = ? WHERE GenreId = ?;",
            [(new_name1, genreId), (new_name2, genreId)],
        )

        cursor.execute(
            "SELECT Name, GenreID FROM genres WHERE GenreId = :id", {"id": genreId}
        )

        assert cursor.fetchone() == (
            new_name2,
            genreId,
        )

    def test_row_factory(self, sqlitecloud_dbapi2_connection):
        connection = sqlitecloud_dbapi2_connection

        connection.row_factory = lambda cursor, row: {
            description[0]: row[i] for i, description in enumerate(cursor.description)
        }

        cursor = connection.execute("SELECT * FROM albums")

        row = cursor.fetchone()

        assert row["AlbumId"] == 1
        assert row["Title"] == "For Those About To Rock We Salute You"
        assert row["ArtistId"] == 1
