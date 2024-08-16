import random
import sqlite3
import time
from datetime import date, datetime

import pytest

import sqlitecloud
from sqlitecloud.datatypes import SQLiteCloudException
from tests.conftest import get_sqlite3_connection, get_sqlitecloud_dbapi2_connection


class TestSQLite3FeatureParity:
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

    @pytest.mark.parametrize(
        "connection", ["sqlitecloud_dbapi2_connection", "sqlite3_connection"]
    )
    def test_row_factory(self, connection, request):
        connection = request.getfixturevalue(connection)

        def simple_factory(cursor, row):
            return {
                description[0]: row[i]
                for i, description in enumerate(cursor.description)
            }

        connection.row_factory = simple_factory

        select_query = "SELECT AlbumId, Title, ArtistId FROM albums WHERE AlbumId = 1"
        cursor = connection.execute(select_query)

        results = cursor.fetchall()

        assert results[0]["AlbumId"] == 1
        assert results[0]["Title"] == "For Those About To Rock We Salute You"
        assert results[0]["ArtistId"] == 1
        assert connection.row_factory == cursor.row_factory

    @pytest.mark.parametrize(
        "connection", ["sqlitecloud_dbapi2_connection", "sqlite3_connection"]
    )
    def test_cursor_row_factory_as_instance_variable(self, connection, request):
        connection = request.getfixturevalue(connection)

        cursor = connection.execute("SELECT 1")
        cursor.row_factory = lambda c, r: list(r)

        cursor2 = connection.execute("SELECT 1")

        assert cursor.row_factory != cursor2.row_factory

    @pytest.mark.parametrize(
        "connection, module",
        [
            ("sqlitecloud_dbapi2_connection", sqlitecloud),
            ("sqlite3_connection", sqlite3),
        ],
    )
    def test_row_factory_with_row_object(self, connection, module, request):
        connection = request.getfixturevalue(connection)

        connection.row_factory = module.Row

        select_query = "SELECT AlbumId, Title, ArtistId FROM albums WHERE AlbumId = 1"
        cursor = connection.execute(select_query)

        row = cursor.fetchone()

        assert row["AlbumId"] == 1
        assert row["Title"] == "For Those About To Rock We Salute You"
        assert row[1] == row["Title"]
        assert row["Title"] == row["title"]
        assert row.keys() == ["AlbumId", "Title", "ArtistId"]
        assert len(row) == 3
        assert next(iter(row)) == 1  # AlbumId
        assert not row != row
        assert row == row

        cursor = connection.execute(
            "SELECT AlbumId, Title, ArtistId FROM albums WHERE AlbumId = 2"
        )
        other_row = cursor.fetchone()

        assert row != other_row

    @pytest.mark.parametrize(
        "connection",
        [
            "sqlitecloud_dbapi2_connection",
            "sqlite3_connection",
        ],
    )
    def test_description(self, connection, request):
        connection = request.getfixturevalue(connection)

        select_query = "SELECT AlbumId, Title, ArtistId FROM albums WHERE AlbumId = 1"
        cursor = connection.execute(select_query)

        assert cursor.description[0][0] == "AlbumId"
        assert cursor.description[1][0] == "Title"
        assert cursor.description[2][0] == "ArtistId"

    @pytest.mark.parametrize(
        "connection",
        [
            "sqlitecloud_dbapi2_connection",
            "sqlite3_connection",
        ],
    )
    def test_cursor_description_with_explicit_decltype(self, connection, request):
        connection = request.getfixturevalue(connection)

        cursor = connection.execute(
            'SELECT "hello world", "hello" as "my world [sphere]", "hello" "world", "hello" "my world"'
        )

        assert cursor.description[0][0] == '"hello world"'
        assert cursor.description[1][0] == "my world"
        assert cursor.description[2][0] == "world"
        assert cursor.description[3][0] == "my world"

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

    def test_commit_without_any_transaction_does_not_raise_exception(
        self, sqlitecloud_dbapi2_connection, sqlite3_connection
    ):
        for connection in [sqlitecloud_dbapi2_connection, sqlite3_connection]:
            connection.commit()

            assert True

    def test_rollback_without_any_transaction_does_not_raise_exception(
        self, sqlitecloud_dbapi2_connection, sqlite3_connection
    ):
        for connection in [sqlitecloud_dbapi2_connection, sqlite3_connection]:
            connection.rollback()

        assert True

    def test_autocommit_mode_enabled_by_default(
        self, sqlitecloud_dbapi2_connection, sqlite3_connection
    ):
        seed = str(int(time.time()))

        connections = [
            (sqlitecloud_dbapi2_connection, next(get_sqlitecloud_dbapi2_connection())),
            (sqlite3_connection, next(get_sqlite3_connection())),
        ]

        for (connection, control_connection) in connections:
            connection.execute(
                "INSERT INTO albums (Title, ArtistId) VALUES (? , 1);",
                (f"Test {seed}",),
            )

            cursor2 = control_connection.execute(
                "SELECT * FROM albums WHERE Title = ?", (f"Test {seed}",)
            )
            assert cursor2.fetchone() is not None

    def test_explicit_transaction_to_commit(
        self,
        sqlitecloud_dbapi2_connection: sqlitecloud.Connection,
        sqlite3_connection: sqlite3.Connection,
    ):
        seed = str(int(time.time()))

        connections = [
            (sqlitecloud_dbapi2_connection, next(get_sqlitecloud_dbapi2_connection())),
            (sqlite3_connection, next(get_sqlite3_connection())),
        ]

        for (connection, control_connection) in connections:
            cursor1 = connection.execute("BEGIN;")
            cursor1.execute(
                "INSERT INTO albums (Title, ArtistId) VALUES (?, 1);", (f"Test {seed}",)
            )

            cursor2 = control_connection.execute(
                "SELECT * FROM albums WHERE Title = ?", (f"Test {seed}",)
            )
            assert cursor2.fetchone() is None

            connection.commit()

            cursor2.execute("SELECT * FROM albums WHERE Title = ?", (f"Test {seed}",))
            assert cursor2.fetchone() is not None

    def test_explicit_transaction_to_rollback(
        self,
        sqlitecloud_dbapi2_connection: sqlitecloud.Connection,
        sqlite3_connection: sqlite3.Connection,
    ):
        seed = str(int(time.time()))

        for connection in [sqlitecloud_dbapi2_connection, sqlite3_connection]:
            cursor1 = connection.execute("BEGIN;")
            cursor1.execute(
                "INSERT INTO albums (Title, ArtistId) VALUES (?, 1);", (f"Test {seed}",)
            )

            cursor1.execute("SELECT * FROM albums WHERE Title = ?", (f"Test {seed}",))
            assert cursor1.fetchone() is not None

            connection.rollback()

            cursor1.execute("SELECT * FROM albums WHERE Title = ?", (f"Test {seed}",))
            assert cursor1.fetchone() is None

    def test_text_factory_with_default_string(
        self, sqlitecloud_dbapi2_connection, sqlite3_connection
    ):
        for connection in [sqlitecloud_dbapi2_connection, sqlite3_connection]:
            # by default is string: connection.text_factory = str

            austria = "\xd6sterreich"

            cursor = connection.execute("SELECT ?", (austria,))
            result = cursor.fetchone()

            assert result[0] == austria

    def test_text_factory_with_bytes(
        self, sqlitecloud_dbapi2_connection, sqlite3_connection
    ):
        for connection in [sqlitecloud_dbapi2_connection, sqlite3_connection]:
            connection.text_factory = bytes

            austria = "\xd6sterreich"

            cursor = connection.execute("SELECT ?", (austria,))
            result = cursor.fetchone()

            assert type(result[0]) is bytes
            assert result[0] == austria.encode("utf-8")

    def test_text_factory_with_callable(
        self, sqlitecloud_dbapi2_connection, sqlite3_connection
    ):
        for connection in [sqlitecloud_dbapi2_connection, sqlite3_connection]:
            connection.text_factory = lambda x: x.decode("utf-8") + "Foo"

            cursor = connection.execute("SELECT ?", ("bar",))
            result = cursor.fetchone()

            assert result[0] == "barFoo"

    @pytest.mark.parametrize(
        "connection",
        [
            "sqlitecloud_dbapi2_connection",
            "sqlite3_connection",
        ],
    )
    def test_apply_text_factory_to_int_value_with_text_decltype(
        self, connection, request
    ):
        """Expect the text_factory to be applied when the inserted
        value is an integer but the declared type for the column is TEXT."""
        connection = request.getfixturevalue(connection)
        connection.text_factory = bytes

        tableName = "TestTextFactory" + str(random.randint(0, 99999))
        try:
            cursor = connection.execute(f"CREATE TABLE {tableName}(p TEXT)")

            cursor.execute(f"INSERT INTO {tableName}(p) VALUES (?)", (15,))
            cursor.execute(f"SELECT p FROM {tableName}")

            result = cursor.fetchone()

            assert result[0] == b"15"
        finally:
            connection.execute(f"DROP TABLE IF EXISTS {tableName}")

    @pytest.mark.parametrize(
        "connection",
        [
            "sqlitecloud_dbapi2_connection",
            "sqlite3_connection",
        ],
    )
    def test_not_apply_text_factory_to_string_value_without_text_decltype(
        self, connection, request
    ):
        """Expect the text_factory to be not applied when the inserted
        value is a string but the declared type for the column is not TEXT."""

        connection = request.getfixturevalue(connection)
        connection.text_factory = bytes

        tableName = "TestTextFactory" + str(random.randint(0, 99999))
        try:
            cursor = connection.execute(f"CREATE TABLE {tableName}(p INTEGER)")

            cursor.execute(f"INSERT INTO {tableName}(p) VALUES (?)", ("15",))
            cursor.execute(f"SELECT p FROM {tableName}")

            result = cursor.fetchone()

            assert result[0] == 15
        finally:
            connection.execute(f"DROP TABLE IF EXISTS {tableName}")

    @pytest.mark.parametrize(
        "connection, module",
        [
            ("sqlitecloud_dbapi2_connection", sqlitecloud),
            ("sqlite3_connection", sqlite3),
        ],
    )
    def test_register_adapter(self, connection, module, request):
        connection = request.getfixturevalue(connection)

        class Point:
            def __init__(self, x, y):
                self.x, self.y = x, y

        def adapt_point(point):
            return f"{point.x}, {point.y}"

        module.register_adapter(Point, adapt_point)

        p = Point(4.0, -3.2)

        cursor = connection.execute("SELECT ?", (p,))

        result = cursor.fetchone()

        assert result[0] == "4.0, -3.2"

    @pytest.mark.parametrize(
        "connection, module",
        [
            ("sqlitecloud_dbapi2_connection", sqlitecloud),
            ("sqlite3_connection", sqlite3),
        ],
    )
    def test_register_adapter_and_executemany(self, connection, module, request):
        connection = request.getfixturevalue(connection)

        class Point:
            def __init__(self, x, y):
                self.x, self.y = x, y

        def adapt_point(point):
            return f"{point.x}, {point.y}"

        module.register_adapter(Point, adapt_point)

        p1 = Point(4.0, -3.2)
        p2 = Point(2.1, 1.9)

        tableName = "TestParseDeclTypes" + str(random.randint(0, 99999))
        try:
            cursor = connection.execute(f"CREATE TABLE {tableName}(p)")

            cursor.executemany(f"INSERT INTO {tableName}(p) VALUES (?)", [(p1,), (p2,)])
            cursor.execute(f"SELECT p FROM {tableName}")

            result = cursor.fetchall()

            assert result[0][0] == "4.0, -3.2"
            assert result[1][0] == "2.1, 1.9"
        finally:
            connection.execute(f"DROP TABLE IF EXISTS {tableName}")

    @pytest.mark.parametrize(
        "connection, module",
        [
            ("sqlitecloud_dbapi2_connection", sqlitecloud),
            ("sqlite3_connection", sqlite3),
        ],
    )
    def test_register_adapter_on_dict_parameters(self, connection, module, request):
        connection = request.getfixturevalue(connection)

        class Point:
            def __init__(self, x, y):
                self.x, self.y = x, y

        def adapt_point(point):
            return f"{point.x}, {point.y}"

        module.register_adapter(Point, adapt_point)

        p = Point(4.0, -3.2)

        cursor = connection.execute("SELECT :point", {"point": p})

        result = cursor.fetchone()

        assert result[0] == "4.0, -3.2"

    @pytest.mark.parametrize(
        "connection",
        [
            "sqlitecloud_dbapi2_connection",
            "sqlite3_connection",
        ],
    )
    def test_adapter_date(self, connection, request):
        connection = request.getfixturevalue(connection)

        today = date.today()
        cursor = connection.execute("SELECT ?", (today,))

        result = cursor.fetchone()

        assert result[0] == today.isoformat()

    @pytest.mark.parametrize(
        "connection",
        [
            "sqlitecloud_dbapi2_connection",
            "sqlite3_connection",
        ],
    )
    def test_adapter_datetime(self, connection, request):
        connection = request.getfixturevalue(connection)

        now = datetime.now()
        cursor = connection.execute("SELECT ?", (now,))

        result = cursor.fetchone()

        assert result[0] == now.isoformat(" ")

    @pytest.mark.parametrize(
        "connection, module",
        [
            ("sqlitecloud_dbapi2_connection", sqlitecloud),
            ("sqlite3_connection", sqlite3),
        ],
    )
    def test_custom_adapter_datetime(self, connection, module, request):
        connection = request.getfixturevalue(connection)

        def adapt_datetime(ts):
            return time.mktime(ts.timetuple())

        module.register_adapter(datetime, adapt_datetime)

        now = datetime.now()
        cursor = connection.execute("SELECT ?", (now,))

        result = cursor.fetchone()

        assert result[0] == adapt_datetime(now)

    @pytest.mark.parametrize(
        "connection",
        [
            "sqlitecloud_dbapi2_connection",
            "sqlite3_connection",
        ],
    )
    def test_conform_object(self, connection, request):
        connection = request.getfixturevalue(connection)

        class Point:
            def __init__(self, x, y):
                self.x, self.y = x, y

            def __conform__(self, protocol):
                if isinstance(connection, sqlitecloud.Connection):
                    assert protocol is None
                elif isinstance(connection, sqlite3.Connection):
                    assert protocol is sqlite3.PrepareProtocol
                else:
                    pytest.fail("Unknown connection type")

                return f"{self.x};{self.y}"

        p = Point(4.0, -3.2)
        cursor = connection.execute("SELECT ?", (p,))

        result = cursor.fetchone()

        assert result[0] == "4.0;-3.2"

    @pytest.mark.parametrize(
        "connection, module",
        [
            ("sqlitecloud_dbapi2_connection", sqlitecloud),
            ("sqlite3_connection", sqlite3),
        ],
    )
    def test_adapters_to_have_precedence_over_conform_object(
        self, connection, module, request
    ):
        connection = request.getfixturevalue(connection)

        class Point:
            def __init__(self, x, y):
                self.x, self.y = x, y

            def __conform__(self, protocol):
                # 4.0;1.1
                return f"{self.x};{self.y}"

        def adapt_point(point):
            # 4.0, 1.1
            return f"{point.x}, {point.y}"

        module.register_adapter(Point, adapt_point)

        p = Point(4.0, -3.2)
        cursor = connection.execute("SELECT ?", (p,))

        result = cursor.fetchone()

        assert result[0] == "4.0, -3.2"

    @pytest.mark.parametrize(
        "connection, module",
        [
            (
                get_sqlitecloud_dbapi2_connection,
                sqlitecloud,
            ),
            (get_sqlite3_connection, sqlite3),
        ],
    )
    def test_parse_decltypes(self, connection, module):
        connection = next(connection(module.PARSE_DECLTYPES))

        class Point:
            def __init__(self, x, y):
                self.x, self.y = x, y

            def __repr__(self):
                return f"{self.x};{self.y}"

        def convert_point(s):
            x, y = list(map(float, s.split(b";")))
            return Point(x, y)

        module.register_converter("point", convert_point)

        p = Point(4.0, -3.2)

        tableName = "TestParseDeclTypes" + str(random.randint(0, 99999))
        try:
            cursor = connection.execute(f"CREATE TABLE {tableName}(p point)")

            cursor.execute(f"INSERT INTO {tableName}(p) VALUES (?)", (str(p),))
            cursor.execute(f"SELECT p FROM {tableName}")

            result = cursor.fetchone()

            assert isinstance(result[0], Point)
            assert result[0].x == p.x
            assert result[0].y == p.y
        finally:
            connection.execute(f"DROP TABLE IF EXISTS {tableName}")

    @pytest.mark.parametrize(
        "connection, module",
        [
            (get_sqlitecloud_dbapi2_connection, sqlitecloud),
            (get_sqlite3_connection, sqlite3),
        ],
    )
    def test_parse_decltypes_when_decltype_is_not_registered(self, connection, module):
        connection = next(connection(module.PARSE_DECLTYPES))

        tableName = "TestParseDeclTypes" + str(random.randint(0, 99999))
        try:
            cursor = connection.execute(f"CREATE TABLE {tableName}(p point)")

            cursor.execute(f"INSERT INTO {tableName}(p) VALUES (?)", ("1.0,2.0",))
            cursor.execute(f"SELECT p FROM {tableName}")

            result = cursor.fetchone()

            assert result[0] == "1.0,2.0"
        finally:
            connection.execute(f"DROP TABLE IF EXISTS {tableName}")

    @pytest.mark.parametrize(
        "connection, module",
        [
            (get_sqlitecloud_dbapi2_connection, sqlitecloud),
            (get_sqlite3_connection, sqlite3),
        ],
    )
    def test_register_converter_case_insensitive(self, connection, module):
        connection = next(connection(module.PARSE_DECLTYPES))

        module.register_converter("integer", lambda x: int(x.decode("utf-8")) + 7)

        mynumber = 10

        tableName = "TestParseDeclTypes" + str(random.randint(0, 99999))
        try:
            cursor = connection.execute(f"CREATE TABLE {tableName}(p INTEGER)")

            cursor.execute(f"INSERT INTO {tableName}(p) VALUES (?)", (mynumber,))
            cursor.execute(f"SELECT p FROM {tableName}")

            result = cursor.fetchone()

            assert result[0] == 17
        finally:
            connection.execute(f"DROP TABLE IF EXISTS {tableName}")

    @pytest.mark.parametrize(
        "connection, module",
        [
            (get_sqlitecloud_dbapi2_connection, sqlitecloud),
            (get_sqlite3_connection, sqlite3),
        ],
    )
    def test_registered_converter_on_text_decltype_replaces_text_factory(
        self, connection, module
    ):
        """Expect the registered converter to the TEXT decltype to be used in place of the text_factory."""
        connection = next(connection(module.PARSE_DECLTYPES))

        module.register_converter("TEXT", lambda x: x.decode("utf-8") + "Foo")
        connection.text_factory = bytes

        pippo = "Pippo"

        tableName = "TestParseDeclTypes" + str(random.randint(0, 99999))
        try:
            cursor = connection.execute(f"CREATE TABLE {tableName}(p TEXT)")

            cursor.execute(f"INSERT INTO {tableName}(p) VALUES (?)", (pippo,))
            cursor.execute(f"SELECT p FROM {tableName}")

            result = cursor.fetchone()

            assert result[0] == pippo + "Foo"
        finally:
            connection.execute(f"DROP TABLE IF EXISTS {tableName}")

    @pytest.mark.parametrize(
        "connection, module",
        [
            (get_sqlitecloud_dbapi2_connection, sqlitecloud),
            (get_sqlite3_connection, sqlite3),
        ],
    )
    def test_parse_native_decltype(self, connection, module):
        connection = next(connection(module.PARSE_DECLTYPES))

        module.register_converter("INTEGER", lambda x: int(x.decode("utf-8")) + 10)

        mynumber = 10

        tableName = "TestParseDeclTypes" + str(random.randint(0, 99999))
        try:
            cursor = connection.execute(f"CREATE TABLE {tableName}(p INTEGER)")

            cursor.execute(f"INSERT INTO {tableName}(p) VALUES (?)", (mynumber,))
            cursor.execute(f"SELECT p FROM {tableName}")

            result = cursor.fetchone()

            assert result[0] == 20
        finally:
            connection.execute(f"DROP TABLE IF EXISTS {tableName}")

    @pytest.mark.parametrize(
        "connection, module",
        [
            (get_sqlitecloud_dbapi2_connection, sqlitecloud),
            (get_sqlite3_connection, sqlite3),
        ],
    )
    def test_register_adapters_and_converters_for_date_and_datetime_by_default(
        self, connection, module
    ):
        connection = next(connection(module.PARSE_DECLTYPES))

        tableName = "TestParseDeclTypes" + str(random.randint(0, 99999))
        try:
            today = date.today()
            now = datetime.now()

            cursor = connection.execute(
                f"CREATE TABLE {tableName}(d DATE, t timestamp)"
            )

            cursor.execute(
                f"INSERT INTO {tableName}(d, t) VALUES (:date, :timestamp)",
                {"date": today, "timestamp": now},
            )
            cursor.execute(f"SELECT d, t FROM {tableName}")

            result = cursor.fetchone()

            assert isinstance(result[0], date)
            assert isinstance(result[1], datetime)
            assert result[0] == today
            assert result[1] == now
        finally:
            connection.execute(f"DROP TABLE IF EXISTS {tableName}")

    @pytest.mark.parametrize(
        "connection, module",
        [
            (get_sqlitecloud_dbapi2_connection, sqlitecloud),
            (get_sqlite3_connection, sqlite3),
        ],
    )
    def test_adapt_and_convert_custom_decltype(self, connection, module):
        connection = next(connection(module.PARSE_DECLTYPES))

        class Point:
            def __init__(self, x, y):
                self.x, self.y = x, y

        def adapt_point(point):
            return f"{point.x};{point.y}".encode("ascii")

        def convert_point(s):
            x, y = list(map(float, s.split(b";")))
            return Point(x, y)

        module.register_adapter(Point, adapt_point)
        module.register_converter("point", convert_point)

        p = Point(4.0, -3.2)

        tableName = "TestParseDeclTypes" + str(random.randint(0, 99999))
        try:
            cursor = connection.cursor()
            cursor.execute(f"CREATE TABLE {tableName}(p point)")

            cursor.execute(f"INSERT INTO {tableName}(p) VALUES (?)", (p,))
            cursor.execute(f"SELECT p FROM {tableName}")

            result = cursor.fetchone()

            assert isinstance(result[0], Point)
            assert result[0].x == p.x
            assert result[0].y == p.y
        finally:
            connection.execute(f"DROP TABLE IF EXISTS {tableName}")

    @pytest.mark.parametrize(
        "connection, module",
        [
            (get_sqlitecloud_dbapi2_connection, sqlitecloud),
            (get_sqlite3_connection, sqlite3),
        ],
    )
    def test_parse_colnames(self, connection, module):
        connection = next(connection(module.PARSE_COLNAMES))

        class Point:
            def __init__(self, x, y):
                self.x, self.y = x, y

            def __repr__(self):
                return f"{self.x};{self.y}"

        def convert_point(s):
            x, y = list(map(float, s.split(b";")))
            return Point(x, y)

        module.register_converter("point", convert_point)

        p = Point(4.0, -3.2)

        cursor = connection.execute('SELECT ? as "p i [point]"', (str(p),))

        result = cursor.fetchone()

        assert isinstance(result[0], Point)
        assert result[0].x == p.x
        assert result[0].y == p.y

    @pytest.mark.parametrize(
        "connection, module",
        [
            (get_sqlitecloud_dbapi2_connection, sqlitecloud),
            (get_sqlite3_connection, sqlite3),
        ],
    )
    def test_parse_colnames_first_and_then_decltypes(self, connection, module):
        """Expect the PARSE_COLNAMES to have priority over PARSE_DECLTYPES."""

        connection = next(connection(module.PARSE_DECLTYPES | module.PARSE_COLNAMES))

        class Point:
            def __init__(self, x, y):
                self.x, self.y = x, y

            def __repr__(self):
                return f"{self.x};{self.y}"

        def convert_point(s):
            x, y = list(map(float, s.split(b";")))
            return Point(x, y)

        def convert_coordinate(c):
            x, y = list(map(float, c.split(b";")))
            return f"lat: {x}, lng: {y}"

        module.register_converter("point", convert_point)
        module.register_converter("coordinate", convert_coordinate)

        p = Point(4.0, -3.2)

        tableName = "TestParseColnames" + str(random.randint(0, 99999))
        try:
            cursor = connection.cursor()
            cursor.execute(f"CREATE TABLE {tableName}(p point)")

            cursor.execute(f"INSERT INTO {tableName}(p) VALUES (?)", (str(p),))
            cursor.execute(f'SELECT p, p "lat lng [coordinate]" FROM {tableName}')

            result = cursor.fetchone()

            assert isinstance(result[0], Point)
            assert result[0].x == p.x
            assert result[0].y == p.y
            assert result[1] == "lat: 4.0, lng: -3.2"
        finally:
            connection.execute(f"DROP TABLE IF EXISTS {tableName}")

    @pytest.mark.parametrize(
        "connection, module",
        [
            (get_sqlitecloud_dbapi2_connection, sqlitecloud),
            (get_sqlite3_connection, sqlite3),
        ],
    )
    def test_parse_colnames_and_decltypes_when_both_are_not_specified(
        self, connection, module
    ):
        connection = next(connection(module.PARSE_DECLTYPES | module.PARSE_COLNAMES))

        cursor = connection.cursor()

        cursor.execute('SELECT 12, 25 "lat lng [coordinate]"')

        result = cursor.fetchone()

        assert result[0] == 12
        assert result[1] == 25
