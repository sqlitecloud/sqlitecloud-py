import os
import random
import time

import pytest

from sqlitecloud.client import SQLiteCloudClient
from sqlitecloud.datatypes import (
    SQLITECLOUD_ERRCODE,
    SQLITECLOUD_INTERNAL_ERRCODE,
    SQLiteCloudAccount,
    SQLiteCloudConnect,
)
from sqlitecloud.exceptions import (
    SQLiteCloudError,
    SQLiteCloudException,
    SQLiteCloudOperationalError,
)
from sqlitecloud.resultset import SQLITECLOUD_RESULT_TYPE


class TestClient:
    # Will warn if a query or other basic operation is slower than this
    WARN_SPEED_MS = 500

    # Will except queries to be quicker than this
    EXPECT_SPEED_MS = 6 * 1000

    def test_connection_with_credentials(self):
        account = SQLiteCloudAccount()
        account.username = os.getenv("SQLITE_USER")
        account.password = os.getenv("SQLITE_PASSWORD")
        account.dbname = os.getenv("SQLITE_DB")
        account.hostname = os.getenv("SQLITE_HOST")
        account.port = int(os.getenv("SQLITE_PORT"))

        client = SQLiteCloudClient(cloud_account=account)
        conn = client.open_connection()
        assert isinstance(conn, SQLiteCloudConnect)

        client.disconnect(conn)

    def test_connection_with_apikey(self):
        account = SQLiteCloudAccount()
        account.apikey = os.getenv("SQLITE_API_KEY")
        account.dbname = os.getenv("SQLITE_DB")
        account.hostname = os.getenv("SQLITE_HOST")
        account.port = int(os.getenv("SQLITE_PORT"))

        client = SQLiteCloudClient(cloud_account=account)
        conn = client.open_connection()
        assert isinstance(conn, SQLiteCloudConnect)

        client.disconnect(conn)

    def test_connection_with_access_token(self):
        account = SQLiteCloudAccount()
        account.token = os.getenv("SQLITE_ACCESS_TOKEN")
        account.dbname = os.getenv("SQLITE_DB")
        account.hostname = os.getenv("SQLITE_HOST")
        account.port = int(os.getenv("SQLITE_PORT"))

        client = SQLiteCloudClient(cloud_account=account)
        conn = client.open_connection()
        assert isinstance(conn, SQLiteCloudConnect)

        client.disconnect(conn)

    def test_connection_without_credentials_and_apikey(self):
        account = SQLiteCloudAccount()
        account.dbname = os.getenv("SQLITE_DB")
        account.hostname = os.getenv("SQLITE_HOST")
        account.port = int(os.getenv("SQLITE_PORT"))

        client = SQLiteCloudClient(cloud_account=account)

        with pytest.raises(SQLiteCloudError):
            client.open_connection()

    def test_connect_with_string(self):
        connection_string = os.getenv("SQLITE_CONNECTION_STRING")

        client = SQLiteCloudClient(connection_str=connection_string)

        conn = client.open_connection()
        assert isinstance(conn, SQLiteCloudConnect)

        client.disconnect(conn)

    def test_connect_with_string_with_credentials(self):
        connection_string = f"sqlitecloud://{os.getenv('SQLITE_USER')}:{os.getenv('SQLITE_PASSWORD')}@{os.getenv('SQLITE_HOST')}/{os.getenv('SQLITE_DB')}"

        client = SQLiteCloudClient(connection_str=connection_string)

        conn = client.open_connection()
        assert isinstance(conn, SQLiteCloudConnect)

        client.disconnect(conn)

    def test_is_connected(self):
        account = SQLiteCloudAccount()
        account.username = os.getenv("SQLITE_API_KEY")
        account.hostname = os.getenv("SQLITE_HOST")
        account.port = int(os.getenv("SQLITE_PORT"))

        client = SQLiteCloudClient(cloud_account=account)

        conn = client.open_connection()
        assert client.is_connected(conn)

        client.disconnect(conn)
        assert not client.is_connected(conn)

    def test_disconnect(self):
        account = SQLiteCloudAccount()
        account.username = os.getenv("SQLITE_API_KEY")
        account.hostname = os.getenv("SQLITE_HOST")
        account.port = int(os.getenv("SQLITE_PORT"))

        client = SQLiteCloudClient(cloud_account=account)

        conn = client.open_connection()
        assert client.is_connected(conn)

        client.disconnect(conn)
        assert not client.is_connected(conn)
        assert conn.socket is None
        assert conn.pubsub_socket is None

        # disconnecting a second time should not raise an exception
        client.disconnect(conn)

    def test_select(self, sqlitecloud_connection):
        connection, client = sqlitecloud_connection

        result = client.exec_query("SELECT 'Hello'", connection)

        assert not result.is_result
        assert 1 == result.nrows
        assert 1 == result.ncols
        assert "Hello" == result.get_value(0, 0)

    def test_column_not_found(self, sqlitecloud_connection):
        connection, client = sqlitecloud_connection
        with pytest.raises(SQLiteCloudOperationalError) as e:
            client.exec_query("SELECT not_a_column FROM albums", connection)

        assert e.value.errcode == 1
        assert e.value.errmsg == "no such column: not_a_column"

    def test_rowset_data(self, sqlitecloud_connection):
        connection, client = sqlitecloud_connection
        result = client.exec_query("SELECT AlbumId FROM albums LIMIT 2", connection)

        assert SQLITECLOUD_RESULT_TYPE.RESULT_ROWSET == result.tag
        assert 2 == result.nrows
        assert 1 == result.ncols
        assert 2 == result.version

    def test_get_value(self, sqlitecloud_connection):
        connection, client = sqlitecloud_connection
        result = client.exec_query("SELECT * FROM albums", connection)

        assert 1 == result.get_value(0, 0)
        assert "For Those About To Rock We Salute You" == result.get_value(0, 1)
        assert 2 == result.get_value(1, 0)

    def test_select_utf8_value_and_column_name(self, sqlitecloud_connection):
        connection, client = sqlitecloud_connection
        result = client.exec_query("SELECT 'Minha História'", connection)

        assert result.nrows == 1
        assert result.ncols == 1
        assert "Minha História" == result.get_value(0, 0)
        assert "'Minha História'" == result.get_name(0)

    def test_invalid_row_number_for_value(self, sqlitecloud_connection):
        connection, client = sqlitecloud_connection
        result = client.exec_query("SELECT 'one row'", connection)

        assert result.get_value(1, 1) is None

    def test_column_name(self, sqlitecloud_connection):
        connection, client = sqlitecloud_connection
        result = client.exec_query("SELECT * FROM albums", connection)

        assert "AlbumId" == result.get_name(0)
        assert "Title" == result.get_name(1)

    def test_invalid_row_number_for_column_name(self, sqlitecloud_connection):
        connection, client = sqlitecloud_connection
        result = client.exec_query("SELECT 'one column'", connection)

        assert result.get_name(2) is None

    def test_long_string(self, sqlitecloud_connection):
        connection, client = sqlitecloud_connection

        size = 1024 * 1024
        value = "LOOOONG"
        while len(value) < size:
            value += "a"

        rowset = client.exec_query(f"SELECT '{value}' 'VALUE'", connection)

        assert 1 == rowset.nrows
        assert 1 == rowset.ncols
        assert "VALUE" == rowset.get_name(0)
        assert value == rowset.get_value(0, 0)

    def test_integer(self, sqlitecloud_connection):
        connection, client = sqlitecloud_connection
        result = client.exec_query("TEST INTEGER", connection)

        assert SQLITECLOUD_RESULT_TYPE.RESULT_INTEGER == result.tag
        assert 123456 == result.get_result()

    def test_integer_64_bit(self, sqlitecloud_connection):
        connection, client = sqlitecloud_connection
        result = client.exec_query("TEST INT64", connection)

        assert SQLITECLOUD_RESULT_TYPE.RESULT_INTEGER == result.tag
        assert 9223372036854775807 == result.get_result()

    def test_float(self, sqlitecloud_connection):
        connection, client = sqlitecloud_connection
        result = client.exec_query("TEST FLOAT", connection)

        assert SQLITECLOUD_RESULT_TYPE.RESULT_FLOAT == result.tag
        assert 3.1415926 == result.get_result()

    def test_string(self, sqlitecloud_connection):
        connection, client = sqlitecloud_connection
        result = client.exec_query("TEST STRING", connection)

        assert SQLITECLOUD_RESULT_TYPE.RESULT_STRING == result.tag
        assert result.get_result() == "Hello World, this is a test string."

    def test_zero_string(self, sqlitecloud_connection):
        connection, client = sqlitecloud_connection
        result = client.exec_query("TEST ZERO_STRING", connection)

        assert SQLITECLOUD_RESULT_TYPE.RESULT_STRING == result.tag
        assert (
            result.get_result() == "Hello World, this is a zero-terminated test string."
        )

    def test_empty_string(self, sqlitecloud_connection):
        connection, client = sqlitecloud_connection
        result = client.exec_query("TEST STRING0", connection)

        assert SQLITECLOUD_RESULT_TYPE.RESULT_STRING == result.tag
        assert result.get_result() == ""

    def test_command(self, sqlitecloud_connection):
        connection, client = sqlitecloud_connection
        result = client.exec_query("TEST COMMAND", connection)

        assert "PONG" == result.get_result()

    def test_json(self, sqlitecloud_connection):
        connection, client = sqlitecloud_connection
        result = client.exec_query("TEST JSON", connection)

        assert SQLITECLOUD_RESULT_TYPE.RESULT_JSON == result.tag
        assert {
            "msg-from": {"class": "soldier", "name": "Wixilav"},
            "msg-to": {"class": "supreme-commander", "name": "[Redacted]"},
            "msg-type": ["0xdeadbeef", "irc log"],
            "msg-log": [
                "soldier: Boss there is a slight problem with the piece offering to humans",
                "supreme-commander: Explain yourself soldier!",
                "soldier: Well they don't seem to move anymore...",
                "supreme-commander: Oh snap, I came here to see them twerk!",
            ],
        } == result.get_result()

    def test_blob(self, sqlitecloud_connection):
        connection, client = sqlitecloud_connection
        result = client.exec_query("TEST BLOB", connection)

        assert SQLITECLOUD_RESULT_TYPE.RESULT_BLOB == result.tag
        assert len(result.get_result()) == 1000

    def test_blob_zero_length(self, sqlitecloud_connection):
        connection, client = sqlitecloud_connection
        result = client.exec_query("TEST BLOB0", connection)

        assert SQLITECLOUD_RESULT_TYPE.RESULT_STRING == result.tag
        assert len(result.get_result()) == 0

    def test_error(self, sqlitecloud_connection):
        connection, client = sqlitecloud_connection

        with pytest.raises(SQLiteCloudError) as e:
            client.exec_query("TEST ERROR", connection)

        assert e.value.errcode == 66666
        assert e.value.errmsg == "This is a test error message with a devil error code."

    def test_ext_error(self, sqlitecloud_connection):
        connection, client = sqlitecloud_connection

        with pytest.raises(SQLiteCloudError) as e:
            client.exec_query("TEST EXTERROR", connection)

        assert e.value.errcode == 66666
        assert e.value.xerrcode == 333
        assert (
            e.value.errmsg
            == "This is a test error message with an extcode and a devil error code."
        )

    def test_array(self, sqlitecloud_connection):
        connection, client = sqlitecloud_connection
        result = client.exec_query("TEST ARRAY", connection)

        result_array = result.get_result()

        assert SQLITECLOUD_RESULT_TYPE.RESULT_ARRAY == result.tag
        assert isinstance(result_array, list)
        assert len(result_array) == 5
        assert result_array[0] == "Hello World"
        assert result_array[1] == 123456
        assert result_array[2] == 3.1415
        assert result_array[3] is None

    def test_rowset(self, sqlitecloud_connection):
        connection, client = sqlitecloud_connection
        result = client.exec_query("TEST ROWSET", connection)

        assert SQLITECLOUD_RESULT_TYPE.RESULT_ROWSET == result.tag
        assert result.nrows >= 30
        assert result.ncols == 2
        assert result.version in [1, 2]
        assert result.get_name(0) == "key"
        assert result.get_name(1) == "value"

    def test_rowset_data_types(self, sqlitecloud_connection):
        connection, client = sqlitecloud_connection

        bindings = ("hello world", 15175, 3.14, b"bytes world", None)
        result = client.exec_statement("SELECT ?, ?, ?, ?, ?", bindings, connection)

        assert SQLITECLOUD_RESULT_TYPE.RESULT_ROWSET == result.tag
        assert result.get_value(0, 0) == "hello world"
        assert result.get_value(0, 1) == 15175
        assert result.get_value(0, 2) == 3.14
        assert result.get_value(0, 3) == b"bytes world"
        assert result.get_value(0, 4) is None

    def test_max_rows_option(self):
        account = SQLiteCloudAccount()
        account.hostname = os.getenv("SQLITE_HOST")
        account.dbname = os.getenv("SQLITE_DB")
        account.apikey = os.getenv("SQLITE_API_KEY")

        client = SQLiteCloudClient(cloud_account=account)
        client.config.maxrows = 1

        connection = client.open_connection()

        rowset = client.exec_query("TEST ROWSET_CHUNK", connection)

        client.disconnect(connection)

        # maxrows cannot be tested at this level.
        # just expect everything is ok
        assert rowset.nrows > 100

    def test_max_rowset_option_to_fail_when_rowset_is_bigger(self):
        account = SQLiteCloudAccount()
        account.hostname = os.getenv("SQLITE_HOST")
        account.dbname = os.getenv("SQLITE_DB")
        account.apikey = os.getenv("SQLITE_API_KEY")

        client = SQLiteCloudClient(cloud_account=account)
        client.config.maxrowset = 1024

        connection = client.open_connection()

        with pytest.raises(SQLiteCloudError) as e:
            client.exec_query("SELECT * FROM albums", connection)

        client.disconnect(connection)

        assert SQLITECLOUD_ERRCODE.INTERNAL.value == e.value.errcode
        assert "RowSet too big to be sent (limit set to 1024 bytes)." == e.value.errmsg

    def test_max_rowset_option_to_succeed_when_rowset_is_lighter(self):
        account = SQLiteCloudAccount()
        account.hostname = os.getenv("SQLITE_HOST")
        account.dbname = os.getenv("SQLITE_DB")
        account.apikey = os.getenv("SQLITE_API_KEY")

        client = SQLiteCloudClient(cloud_account=account)
        client.config.maxrowset = 1024

        connection = client.open_connection()

        rowset = client.exec_query("SELECT 'hello world'", connection)

        client.disconnect(connection)

        assert 1 == rowset.nrows

    def test_rowset_chunk(self, sqlitecloud_connection):
        connection, client = sqlitecloud_connection

        rowset = client.exec_query("TEST ROWSET_CHUNK", connection)

        assert SQLITECLOUD_RESULT_TYPE.RESULT_ROWSET == rowset.tag
        assert 147 == rowset.nrows
        assert 1 == rowset.ncols
        assert 147 == len(rowset.data)
        assert "key" == rowset.get_name(0)

    def test_rowset_nochunk(self, sqlitecloud_connection):
        connection, client = sqlitecloud_connection

        rowset = client.exec_query("TEST ROWSET_NOCHUNK", connection)

        assert SQLITECLOUD_RESULT_TYPE.RESULT_ROWSET == rowset.tag
        assert 147 == rowset.nrows
        assert 1 == rowset.ncols
        assert 147 == len(rowset.data)
        assert "key" == rowset.get_name(0)

    def test_chunked_rowset_twice(self, sqlitecloud_connection):
        connection, client = sqlitecloud_connection
        rowset = client.exec_query("TEST ROWSET_CHUNK", connection)

        assert 147 == rowset.nrows
        assert 1 == rowset.ncols
        assert "key" == rowset.get_name(0)

        rowset = client.exec_query("TEST ROWSET_CHUNK", connection)

        assert 147 == rowset.nrows
        assert 1 == rowset.ncols
        assert "key" == rowset.get_name(0)

        rowset = client.exec_query("SELECT 1", connection)

        assert 1 == rowset.nrows

    def test_serialized_operations(self, sqlitecloud_connection):
        num_queries = 20

        connection, client = sqlitecloud_connection

        for i in range(num_queries):
            rowset = client.exec_query(
                f"select {i} as 'count', 'hello' as 'string'", connection
            )

            assert 1 == rowset.nrows
            assert 2 == rowset.ncols
            assert "count" == rowset.get_name(0)
            assert "string" == rowset.get_name(1)
            assert i == rowset.get_value(0, 0)
            assert rowset.version in [1, 2]

    def test_query_timeout(self):
        account = SQLiteCloudAccount()
        account.hostname = os.getenv("SQLITE_HOST")
        account.dbname = os.getenv("SQLITE_DB")
        account.apikey = os.getenv("SQLITE_API_KEY")

        client = SQLiteCloudClient(cloud_account=account)
        client.config.timeout = 1  # 1 sec

        connection = client.open_connection()

        # this operation should take more than 1 sec
        with pytest.raises(SQLiteCloudException) as e:
            # just a long running query
            client.exec_query(
                """
                WITH RECURSIVE r(i) AS (
                    VALUES(0)
                    UNION ALL
                    SELECT i FROM r
                    LIMIT 10000000
                )
                SELECT i FROM r WHERE i = 1;""",
                connection,
            )

        client.disconnect(connection)

        assert e.value.errcode == SQLITECLOUD_INTERNAL_ERRCODE.NETWORK
        assert (
            e.value.errmsg
            == "An error occurred while reading command length from the socket."
        )

    def test_XXL_query(self, sqlitecloud_connection):
        connection, client = sqlitecloud_connection

        xxl_query = 300000
        long_sql = ""

        while len(long_sql) < xxl_query:
            for i in range(5000):
                long_sql += f"SELECT {len(long_sql)} 'HowLargeIsTooMuch'; "

            rowset = client.exec_query(long_sql, connection)

            assert 1 == rowset.nrows
            assert 1 == rowset.ncols
            assert len(long_sql) - 50 <= int(rowset.get_value(0, 0))

    def test_single_XXL_query(self, sqlitecloud_connection):
        connection, client = sqlitecloud_connection

        xxl_query = 200000
        long_sql = ""

        while len(long_sql) < xxl_query:
            long_sql += str(len(long_sql)) + "_"
        selected_value = f"start_{long_sql}end"
        long_sql = f"SELECT '{selected_value}'"

        rowset = client.exec_query(long_sql, connection)

        assert 1 == rowset.nrows
        assert 1 == rowset.ncols
        assert f"'{selected_value}'" == rowset.get_name(0)

    def test_metadata(self, sqlitecloud_connection):
        connection, client = sqlitecloud_connection
        rowset = client.exec_query("LIST METADATA", connection)

        assert rowset.nrows >= 32
        assert rowset.ncols == 8

    def test_select_results_with_no_column_name(self, sqlitecloud_connection):
        connection, client = sqlitecloud_connection
        rowset = client.exec_query("SELECT 42, 'hello'", connection)

        assert rowset.nrows == 1
        assert rowset.ncols == 2
        assert rowset.get_name(0) == "42"
        assert rowset.get_name(1) == "'hello'"
        assert rowset.get_value(0, 0) == 42
        assert rowset.get_value(0, 1) == "hello"

    def test_select_long_formatted_string(self, sqlitecloud_connection):
        connection, client = sqlitecloud_connection

        long_string = "x" * 1000
        rowset = client.exec_query(
            f"USE DATABASE :memory:; SELECT '{long_string}' AS DDD", connection
        )

        assert rowset.nrows == 1
        assert rowset.ncols == 1
        assert rowset.get_value(0, 0).startswith("xxxxxxxx")
        assert len(rowset.get_value(0, 0)) == 1000

    def test_select_database(self):
        account = SQLiteCloudAccount()
        account.hostname = os.getenv("SQLITE_HOST")
        account.dbname = ""
        account.apikey = os.getenv("SQLITE_API_KEY")

        client = SQLiteCloudClient(cloud_account=account)

        connection = client.open_connection()

        rowset = client.exec_query("USE DATABASE chinook.sqlite", connection)

        client.disconnect(connection)

        assert rowset.get_result()

    def test_select_tracks_without_limit(self, sqlitecloud_connection):
        connection, client = sqlitecloud_connection
        rowset = client.exec_query("SELECT * FROM tracks", connection)

        assert rowset.nrows >= 3000
        assert rowset.ncols == 9

    def test_select_tracks_with_limit(self, sqlitecloud_connection):
        connection, client = sqlitecloud_connection
        rowset = client.exec_query("SELECT * FROM tracks LIMIT 10", connection)

        assert rowset.nrows == 10
        assert rowset.ncols == 9

    def test_stress_test_20x_string_select_individual(self, sqlitecloud_connection):
        connection, client = sqlitecloud_connection

        num_queries = 20
        completed = 0
        start_time = time.time()

        for i in range(num_queries):
            rowset = client.exec_query("TEST STRING", connection)

            assert rowset.get_result() == "Hello World, this is a test string."

            completed += 1
            if completed >= num_queries:
                query_ms = round((time.time() - start_time) * 1000 / num_queries)
                if query_ms > self.WARN_SPEED_MS:
                    assert (
                        query_ms < self.EXPECT_SPEED_MS
                    ), f"{num_queries}x test string, {query_ms}ms per query"

    def test_stress_test_20x_individual_select(self, sqlitecloud_connection):
        num_queries = 20
        completed = 0
        start_time = time.time()
        connection, client = sqlitecloud_connection

        for i in range(num_queries):
            rowset = client.exec_query(
                "SELECT * FROM albums ORDER BY RANDOM() LIMIT 4", connection
            )

            assert rowset.nrows == 4
            assert rowset.ncols == 3

            completed += 1
            if completed >= num_queries:
                query_ms = round((time.time() - start_time) * 1000 / num_queries)
                if query_ms > self.WARN_SPEED_MS:
                    assert (
                        query_ms < self.EXPECT_SPEED_MS
                    ), f"{num_queries}x individual selects, {query_ms}ms per query"

    def test_stress_test_20x_batched_selects(self, sqlitecloud_connection):
        connection, client = sqlitecloud_connection

        num_queries = 20
        completed = 0
        start_time = time.time()

        for i in range(num_queries):
            rowset = client.exec_query(
                "SELECT * FROM albums ORDER BY RANDOM() LIMIT 16; SELECT * FROM albums ORDER BY RANDOM() LIMIT 12; SELECT * FROM albums ORDER BY RANDOM() LIMIT 8; SELECT * FROM albums ORDER BY RANDOM() LIMIT 4",
                connection,
            )

            assert rowset.nrows == 4
            assert rowset.ncols == 3

            completed += 1
            if completed >= num_queries:
                query_ms = round((time.time() - start_time) * 1000 / num_queries)
                if query_ms > self.WARN_SPEED_MS:
                    assert (
                        query_ms < self.EXPECT_SPEED_MS
                    ), f"{num_queries}x batched selects, {query_ms}ms per query"

    @pytest.mark.slow
    def test_big_rowset(self):
        account = SQLiteCloudAccount()
        account.hostname = os.getenv("SQLITE_HOST")
        account.apikey = os.getenv("SQLITE_API_KEY")
        account.dbname = os.getenv("SQLITE_DB")

        client = SQLiteCloudClient(cloud_account=account)
        client.config.timeout = 120

        connection = client.open_connection()

        table_name = "TestCompress" + str(random.randint(0, 99999))
        try:
            client.exec_query(
                f"CREATE TABLE {table_name} (id INTEGER PRIMARY KEY, name TEXT)",
                connection,
            )

            nRows = 5000

            sql = ""
            for i in range(nRows):
                sql += f"INSERT INTO {table_name} (name) VALUES ('Test-{i}'); "

            client.exec_query(sql, connection)

            rowset = client.exec_query(
                f"SELECT * from {table_name}",
                connection,
            )

            assert rowset.nrows == nRows
        finally:
            client.exec_query(f"DROP TABLE IF EXISTS {table_name}", connection)
            client.disconnect(connection)

    def test_compression_single_column(self):
        account = SQLiteCloudAccount()
        account.hostname = os.getenv("SQLITE_HOST")
        account.apikey = os.getenv("SQLITE_API_KEY")
        account.dbname = os.getenv("SQLITE_DB")

        client = SQLiteCloudClient(cloud_account=account)
        client.config.compression = True

        connection = client.open_connection()

        # min compression size for rowset set by default to 20400 bytes
        blob_size = 20 * 1024
        # rowset = client.exec_query("SELECT * from albums inner join albums a2 on albums.AlbumId = a2.AlbumId")
        rowset = client.exec_query(
            f"SELECT hex(randomblob({blob_size})) AS 'someColumnName'", connection
        )

        client.disconnect(connection)

        assert rowset.nrows == 1
        assert rowset.ncols == 1
        assert rowset.get_name(0) == "someColumnName"
        assert len(rowset.get_value(0, 0)) == blob_size * 2

    def test_compression_multiple_columns(self):
        account = SQLiteCloudAccount()
        account.hostname = os.getenv("SQLITE_HOST")
        account.apikey = os.getenv("SQLITE_API_KEY")
        account.dbname = os.getenv("SQLITE_DB")

        client = SQLiteCloudClient(cloud_account=account)
        client.config.compression = True

        connection = client.open_connection()

        # min compression size for rowset set by default to 20400 bytes
        rowset = client.exec_query(
            "SELECT * from albums inner join albums a2 on albums.AlbumId = a2.AlbumId",
            connection,
        )

        client.disconnect(connection)

        assert rowset.nrows > 0
        assert rowset.ncols > 0
        assert rowset.get_name(0) == "AlbumId"

    def test_rowset_nochunk_compressed(self, sqlitecloud_connection):
        connection, client = sqlitecloud_connection

        rowset = client.exec_query("TEST ROWSET_NOCHUNK_COMPRESSED", connection)

        assert SQLITECLOUD_RESULT_TYPE.RESULT_ROWSET == rowset.tag
        assert 147 == rowset.nrows
        assert 1 == rowset.ncols
        assert 147 == len(rowset.data)
        assert "key" == rowset.get_name(0)

    def test_rowset_chunk_compressed(self, sqlitecloud_connection):
        connection, client = sqlitecloud_connection

        rowset = client.exec_query("TEST ROWSET_CHUNK_COMPRESSED", connection)

        assert SQLITECLOUD_RESULT_TYPE.RESULT_ROWSET == rowset.tag
        assert 147 == rowset.nrows
        assert 1 == rowset.ncols
        assert 147 == len(rowset.data)
        assert "key" == rowset.get_name(0)

    def test_exec_statement_with_qmarks(self, sqlitecloud_connection):
        connection, client = sqlitecloud_connection

        result = client.exec_statement(
            "SELECT * FROM albums WHERE AlbumId = ? and Title = ?",
            (
                1,
                "For Those About To Rock We Salute You",
            ),
            connection,
        )

        assert result.nrows == 1
        assert result.get_value(0, 0) == 1
