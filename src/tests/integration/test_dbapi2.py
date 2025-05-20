import os
import uuid

import pytest

import sqlitecloud
from sqlitecloud.datatypes import SQLiteCloudAccount
from sqlitecloud.exceptions import SQLiteCloudError, SQLiteCloudProgrammingError


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

        with pytest.raises(SQLiteCloudProgrammingError) as e:
            connection.execute("SELECT 1")

        assert e.value.errcode == 1
        assert e.value.errmsg == "The cursor is closed."

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

        with pytest.raises(SQLiteCloudError) as e:
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

        with pytest.raises(SQLiteCloudError) as e:
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

    def test_execute_with_named_placeholder_and_a_fake_one_which_is_not_given(
        self, sqlitecloud_dbapi2_connection
    ):
        """ "Expect the converter from name to qmark placeholder to not be fooled by the
        fake name with the colon in it."""
        connection = sqlitecloud_dbapi2_connection

        cursor = connection.execute(
            "SELECT * FROM albums WHERE AlbumId = :id and Title != 'special:name'",
            {"id": 1},
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

    def test_row_object_for_factory_string_representation(
        self, sqlitecloud_dbapi2_connection
    ):
        connection = sqlitecloud_dbapi2_connection

        connection.row_factory = sqlitecloud.Row

        cursor = connection.execute('SELECT "foo" as Bar, "john" Doe')

        row = cursor.fetchone()

        assert str(row) == "Bar: foo\nDoe: john"

    def test_last_rowid_and_rowcount_with_select(self, sqlitecloud_dbapi2_connection):
        connection = sqlitecloud_dbapi2_connection

        cursor = connection.execute("SELECT * FROM genres LIMIT 3")

        assert cursor.fetchone() is not None
        assert cursor.lastrowid is None
        assert cursor.rowcount == 3

    def test_last_rowid_and_rowcount_with_execute_update(
        self, sqlitecloud_dbapi2_connection
    ):
        connection = sqlitecloud_dbapi2_connection

        new_name = "Jazz" + str(uuid.uuid4())
        genreId = 2

        cursor = connection.execute(
            "UPDATE genres SET Name = ? WHERE GenreId = ?",
            (new_name, genreId),
        )

        assert cursor.fetchone() is None
        assert cursor.lastrowid is None
        assert cursor.rowcount == 1

    def test_last_rowid_and_rowcount_with_execute_insert(
        self, sqlitecloud_dbapi2_connection
    ):
        connection = sqlitecloud_dbapi2_connection

        new_name = "Jazz" + str(uuid.uuid4())

        cursor = connection.execute(
            "INSERT INTO genres (Name) VALUES (?)",
            (new_name,),
        )

        last_result = connection.execute(
            "SELECT GenreId FROM genres WHERE Name = ?", (new_name,)
        )

        assert cursor.fetchone() is None
        assert cursor.lastrowid == last_result.fetchone()[0]
        assert cursor.rowcount == 1

    def test_last_rowid_and_rowcount_with_execute_delete(
        self, sqlitecloud_dbapi2_connection
    ):
        connection = sqlitecloud_dbapi2_connection

        new_name = "Jazz" + str(uuid.uuid4())

        cursor_select = connection.execute(
            "INSERT INTO genres (Name) VALUES (?)",
            (new_name,),
        )

        cursor = connection.execute("DELETE FROM genres WHERE Name = ?", (new_name,))

        assert cursor.fetchone() is None
        assert cursor.lastrowid == cursor_select.lastrowid
        assert cursor.rowcount == 1

    def test_last_rowid_and_rowcount_with_multiple_updates(
        self, sqlitecloud_dbapi2_connection
    ):
        connection = sqlitecloud_dbapi2_connection

        new_name = "Jazz" + str(uuid.uuid4())

        cursor = connection.execute(
            "UPDATE genres SET Name = ? WHERE GenreId = ? or GenreId = ?",
            (new_name, 2, 3),
        )

        assert cursor.fetchone() is None
        assert cursor.lastrowid is None
        assert cursor.rowcount == 2

    def test_last_rowid_and_rowcount_with_multiple_deletes(
        self, sqlitecloud_dbapi2_connection
    ):
        connection = sqlitecloud_dbapi2_connection

        new_name1 = "Jazz" + str(uuid.uuid4())
        new_name2 = "Jazz" + str(uuid.uuid4())

        cursor = connection.executemany(
            "INSERT INTO genres (Name) VALUES (?)",
            [(new_name1,), (new_name2,)],
        )

        cursor = connection.execute(
            "DELETE FROM genres WHERE Name = ? or Name = ?", (new_name1, new_name2)
        )

        assert cursor.fetchone() is None
        assert cursor.lastrowid > 0
        assert cursor.rowcount == 2

    def test_last_rowid_and_rowcount_with_executemany_updates(
        self, sqlitecloud_dbapi2_connection
    ):
        connection = sqlitecloud_dbapi2_connection

        new_name1 = "Jazz" + str(uuid.uuid4())
        new_name2 = "Jazz" + str(uuid.uuid4())
        genreId = 2

        cursor = connection.executemany(
            "UPDATE genres SET Name = ? WHERE GenreId = ?;",
            [(new_name1, genreId), (new_name2, genreId)],
        )

        assert cursor.fetchone() is None
        assert cursor.lastrowid is None
        assert cursor.rowcount == 1

    def test_last_rowid_and_rowcount_with_executemany_inserts(
        self, sqlitecloud_dbapi2_connection
    ):
        connection = sqlitecloud_dbapi2_connection

        new_name1 = "Jazz" + str(uuid.uuid4())
        new_name2 = "Jazz" + str(uuid.uuid4())

        cursor = connection.executemany(
            "INSERT INTO genres (Name) VALUES (?)",
            [(new_name1,), (new_name2,)],
        )

        last_result = connection.execute(
            "SELECT GenreId FROM genres WHERE Name = ?", (new_name2,)
        )

        assert cursor.fetchone() is None
        assert cursor.lastrowid == last_result.fetchone()[0]
        assert cursor.rowcount == 1

    def test_last_rowid_and_rowcount_with_executemany_deletes(
        self, sqlitecloud_dbapi2_connection
    ):
        connection = sqlitecloud_dbapi2_connection

        new_name1 = "Jazz" + str(uuid.uuid4())
        new_name2 = "Jazz" + str(uuid.uuid4())

        cursor_insert = connection.executemany(
            "INSERT INTO genres (Name) VALUES (?)",
            [(new_name1,), (new_name2,)],
        )

        cursor = connection.executemany(
            "DELETE FROM genres WHERE Name = ?", [(new_name1,), (new_name2,)]
        )

        assert cursor.fetchone() is None
        assert cursor.lastrowid == cursor_insert.lastrowid
        assert cursor.rowcount == 1

    def test_connection_is_connected(self, sqlitecloud_dbapi2_connection):
        connection = sqlitecloud_dbapi2_connection

        assert connection.is_connected()

        connection.close()

        assert not connection.is_connected()

    def test_fetchall_returns_right_nrows_number(self, sqlitecloud_dbapi2_connection):
        connection = sqlitecloud_dbapi2_connection

        cursor = connection.cursor()

        cursor.execute("SELECT * FROM Genres LIMIT 3")

        assert len(cursor.fetchall()) == 3
        assert cursor.rowcount == 3

        cursor.execute("SELECT * FROM Albums LIMIT 4")

        assert len(cursor.fetchall()) == 4
        assert cursor.rowcount == 4
