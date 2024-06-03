import os
import sqlite3

import pytest

from sqlitecloud.types import SQLiteCloudException


class TestSQLite3FeatureParity:
    @pytest.fixture()
    def sqlite3_connection(self):
        connection = sqlite3.connect(
            os.path.join(os.path.dirname(__file__), "../assets/chinook.sqlite")
        )
        yield connection
        connection.close()

    def test_connection_close(self, sqlitecloud_dbapi2_connection, sqlite3_connection):
        sqlitecloud_connection = sqlitecloud_dbapi2_connection

        sqlitecloud_connection.close()
        sqlite3_connection.close()

        with pytest.raises(SQLiteCloudException) as e:
            sqlitecloud_connection.execute("SELECT 1")

        assert isinstance(e.value, SQLiteCloudException)

        with pytest.raises(sqlite3.ProgrammingError) as e:
            sqlite3_connection.execute("SELECT 1")

        assert isinstance(e.value, sqlite3.ProgrammingError)

    @pytest.mark.skip(
        reason="SQLite Cloud does not convert to int a column without an explicit SQLite Type"
    )
    def test_ping_select(self, sqlitecloud_dbapi2_connection, sqlite3_connection):
        sqlitecloud_connection = sqlitecloud_dbapi2_connection

        sqlitecloud_cursor = sqlitecloud_connection.execute("SELECT 1")
        sqlite3_cursor = sqlite3_connection.execute("SELECT 1")

        sqlitecloud_cursor = sqlitecloud_cursor.fetchall()
        sqlite3_cursor = sqlite3_cursor.fetchall()

        assert sqlitecloud_cursor == sqlite3_cursor

    def test_create_table_and_insert_many(
        self, sqlitecloud_dbapi2_connection, sqlite3_connection
    ):
        sqlitecloud_connection = sqlitecloud_dbapi2_connection

        create_table_query = "CREATE TABLE IF NOT EXISTS sqlitetest (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)"
        sqlitecloud_connection.execute(create_table_query)
        sqlite3_connection.execute(create_table_query)

        truncate_table_query = "DELETE FROM sqlitetest"
        sqlitecloud_connection.execute(truncate_table_query)
        sqlite3_connection.execute(truncate_table_query)

        insert_query = "INSERT INTO sqlitetest (name, age) VALUES (?, ?)"
        params = [("Alice", 25), ("Bob", 30)]
        sqlitecloud_connection.executemany(insert_query, params)
        sqlite3_connection.executemany(insert_query, params)

        select_query = "SELECT * FROM sqlitetest"
        sqlitecloud_cursor = sqlitecloud_connection.execute(select_query)
        sqlite3_cursor = sqlite3_connection.execute(select_query)

        sqlitecloud_results = sqlitecloud_cursor.fetchall()
        sqlite3_results = sqlite3_cursor.fetchall()

        assert sqlitecloud_results == sqlite3_results

    def test_execute_with_question_mark_style(
        self, sqlitecloud_dbapi2_connection, sqlite3_connection
    ):
        sqlitecloud_connection = sqlitecloud_dbapi2_connection

        select_query = "SELECT * FROM albums WHERE AlbumId = ?"
        params = (1,)
        sqlitecloud_cursor = sqlitecloud_connection.execute(select_query, params)
        sqlite3_cursor = sqlite3_connection.execute(select_query, params)

        sqlitecloud_results = sqlitecloud_cursor.fetchall()
        sqlite3_results = sqlite3_cursor.fetchall()

        assert sqlitecloud_results == sqlite3_results

    def test_execute_with_named_param_style(
        self, sqlitecloud_dbapi2_connection, sqlite3_connection
    ):
        sqlitecloud_connection = sqlitecloud_dbapi2_connection

        select_query = "SELECT * FROM albums WHERE AlbumId = :id"
        params = {"id": 1}
        sqlitecloud_cursor = sqlitecloud_connection.execute(select_query, params)
        sqlite3_cursor = sqlite3_connection.execute(select_query, params)

        sqlitecloud_results = sqlitecloud_cursor.fetchall()
        sqlite3_results = sqlite3_cursor.fetchall()

        assert sqlitecloud_results == sqlite3_results

    @pytest.mark.skip(
        reason="Rowcount does not contain the number of inserted rows yet"
    )
    def test_insert_result(self, sqlitecloud_dbapi2_connection, sqlite3_connection):
        sqlitecloud_connection = sqlitecloud_dbapi2_connection

        insert_query = "INSERT INTO albums (Title, ArtistId) VALUES (?, ?)"
        params = ("Test Album", 1)
        sqlitecloud_cursor = sqlitecloud_connection.execute(insert_query, params)
        sqlite3_cursor = sqlite3_connection.execute(insert_query, params)

        assert sqlitecloud_cursor.rowcount == sqlite3_cursor.rowcount

    def test_close_cursor_raises_exception(
        self, sqlitecloud_dbapi2_connection, sqlite3_connection
    ):
        sqlitecloud_connection = sqlitecloud_dbapi2_connection

        select_query = "SELECT 1"
        sqlitecloud_cursor = sqlitecloud_connection.execute(select_query)
        sqlite3_cursor = sqlite3_connection.execute(select_query)

        sqlitecloud_cursor.close()
        sqlite3_cursor.close()

        with pytest.raises(SQLiteCloudException) as e:
            sqlitecloud_cursor.fetchall()

        assert isinstance(e.value, SQLiteCloudException)

        with pytest.raises(sqlite3.ProgrammingError) as e:
            sqlite3_cursor.fetchall()

    def test_row_factory(self, sqlitecloud_dbapi2_connection, sqlite3_connection):
        sqlitecloud_connection = sqlitecloud_dbapi2_connection

        def simple_factory(cursor, row):
            return {
                description[0]: row[i]
                for i, description in enumerate(cursor.description)
            }

        sqlitecloud_connection.row_factory = simple_factory
        sqlite3_connection.row_factory = simple_factory

        select_query = "SELECT * FROM albums WHERE AlbumId = 1"
        sqlitecloud_cursor = sqlitecloud_connection.execute(select_query)
        sqlite3_cursor = sqlite3_connection.execute(select_query)

        sqlitecloud_results = sqlitecloud_cursor.fetchall()
        sqlite3_results = sqlite3_cursor.fetchall()

        assert sqlitecloud_results == sqlite3_results
        assert sqlitecloud_results[0]["Title"] == sqlite3_results[0]["Title"]

    def test_description(self, sqlitecloud_dbapi2_connection, sqlite3_connection):
        sqlitecloud_connection = sqlitecloud_dbapi2_connection

        select_query = "SELECT * FROM albums WHERE AlbumId = 1"
        sqlitecloud_cursor = sqlitecloud_connection.execute(select_query)
        sqlite3_cursor = sqlite3_connection.execute(select_query)

        assert sqlitecloud_cursor.description == sqlite3_cursor.description
        assert sqlitecloud_cursor.description[1][0] == "Title"
        assert sqlite3_cursor.description[1][0] == "Title"

    def test_fetch_one(self, sqlitecloud_dbapi2_connection, sqlite3_connection):
        sqlitecloud_connection = sqlitecloud_dbapi2_connection

        select_query = "SELECT * FROM albums WHERE AlbumId = 1"
        sqlitecloud_cursor = sqlitecloud_connection.execute(select_query)
        sqlite3_cursor = sqlite3_connection.execute(select_query)

        sqlitecloud_result = sqlitecloud_cursor.fetchone()
        sqlite3_result = sqlite3_cursor.fetchone()

        assert sqlitecloud_result == sqlite3_result

        sqlitecloud_result = sqlitecloud_cursor.fetchone()
        sqlite3_result = sqlite3_cursor.fetchone()

        assert sqlitecloud_result is None
        assert sqlite3_result is None

    def test_fatchmany(self, sqlitecloud_dbapi2_connection, sqlite3_connection):
        sqlitecloud_connection = sqlitecloud_dbapi2_connection

        select_query = "SELECT * FROM albums"
        sqlitecloud_cursor = sqlitecloud_connection.execute(select_query)
        sqlite3_cursor = sqlite3_connection.execute(select_query)

        sqlitecloud_results = sqlitecloud_cursor.fetchmany(2)
        sqlite3_results = sqlite3_cursor.fetchmany(2)

        assert len(sqlitecloud_results) == 2
        assert len(sqlite3_results) == 2
        assert sqlitecloud_results == sqlite3_results

    def test_fetchmany_more_then_available(
        self, sqlitecloud_dbapi2_connection, sqlite3_connection
    ):
        sqlitecloud_connection = sqlitecloud_dbapi2_connection

        select_query = "SELECT * FROM albums LIMIT 3"
        sqlitecloud_cursor = sqlitecloud_connection.execute(select_query)
        sqlite3_cursor = sqlite3_connection.execute(select_query)

        sqlitecloud_results = sqlitecloud_cursor.fetchmany(100)
        sqlite3_results = sqlite3_cursor.fetchmany(100)

        assert sqlitecloud_results == sqlite3_results
        assert len(sqlitecloud_results) == 3
        assert len(sqlite3_results) == 3

        sqlitecloud_results = sqlitecloud_cursor.fetchmany(100)
        sqlite3_results = sqlite3_cursor.fetchmany(100)

        assert sqlitecloud_results == sqlite3_results
        assert len(sqlitecloud_results) == 0
        assert len(sqlite3_results) == 0

    def test_fetchall(self, sqlitecloud_dbapi2_connection, sqlite3_connection):
        sqlitecloud_connection = sqlitecloud_dbapi2_connection

        select_query = "SELECT * FROM albums LIMIT 5"
        sqlitecloud_cursor = sqlitecloud_connection.execute(select_query)
        sqlite3_cursor = sqlite3_connection.execute(select_query)

        sqlitecloud_results = sqlitecloud_cursor.fetchall()
        sqlite3_results = sqlite3_cursor.fetchall()

        assert sqlitecloud_results == sqlite3_results
        assert len(sqlitecloud_results) == 5
        assert len(sqlite3_results) == 5

        sqlitecloud_results = sqlitecloud_cursor.fetchall()
        sqlite3_results = sqlite3_cursor.fetchall()

        assert sqlitecloud_results == sqlite3_results
        assert len(sqlitecloud_results) == 0
        assert len(sqlite3_results) == 0
