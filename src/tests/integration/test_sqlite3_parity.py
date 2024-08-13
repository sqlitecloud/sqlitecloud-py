import sqlite3
import time
from datetime import date, datetime

import pytest

import sqlitecloud
from sqlitecloud.datatypes import SQLiteCloudException
from tests.conftest import get_sqlite3_connection, get_sqlitecloud_dbapi2_connection


class TestSQLite3FeatureParity:
    @pytest.fixture()
    def sqlite3_connection(self):
        yield next(get_sqlite3_connection())

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
            (sqlite3_connection, next(self.get_sqlite3_connection())),
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
            (sqlite3_connection, next(self.get_sqlite3_connection())),
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
            # by default is string
            # connection.text_factory = str

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

    # def test_datatypes(self, sqlite3_connection):
    #     class Point:
    #         def __init__(self, x, y):
    #             self.x, self.y = x, y

    #         def __repr__(self):
    #             return "(%f;%f)" % (self.x, self.y)

    #     def adapt_point(point):
    #         return ("%f;%f" % (point.x, point.y)).encode('ascii')

    #     def convert_point(s):
    #         x, y = list(map(float, s.split(b";")))
    #         return Point(x, y)

    #     # Register the adapter
    #     sqlite3.register_adapter(Point, adapt_point)

    #     # Register the converter
    #     sqlite3.register_converter("point", convert_point)

    #     p = Point(4.0, -3.2)

    #     #########################
    #     # 1) Using declared types
    #     con = sqlite3.connect(":memory:", detect_types=sqlite3.PARSE_DECLTYPES)
    #     cur = con.cursor()
    #     cur.execute("create table test(p point)")

    #     cur.execute("insert into test(p) values (?)", (p,))
    #     cur.execute("select p from test")
    #     r = cur.fetchone()
    #     print("with declared types:", r[0])
    #     cur.close()
    #     con.close()

    #     #######################
    #     # 1) Using column names
    #     con = sqlite3.connect(":memory:", detect_types=sqlite3.PARSE_COLNAMES)
    #     cur = con.cursor()
    #     cur.execute("create table test(p)")

    #     cur.execute("insert into test(p) values (?)", (p,))
    #     cur.execute('select p as "p [point]" from test')
    #     r = cur.fetchone()
    #     print("with column names:", r[0])
    #     cur.close()
    #     con.close()
