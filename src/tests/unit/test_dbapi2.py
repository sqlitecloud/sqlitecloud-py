import pytest
from pytest_mock import MockerFixture

import sqlitecloud
from sqlitecloud import Cursor
from sqlitecloud.datatypes import (
    SQLiteCloudAccount,
    SQLiteCloudConfig,
    SQLiteCloudException,
)
from sqlitecloud.dbapi2 import Connection
from sqlitecloud.driver import Driver
from sqlitecloud.resultset import SQLITECLOUD_RESULT_TYPE, SQLiteCloudResult


def test_connect_with_account_and_config(mocker: MockerFixture):
    mock_connect = mocker.patch("sqlitecloud.driver.Driver.connect")

    account = SQLiteCloudAccount()
    account.hostname = "myhost"
    account.port = 1234

    config = SQLiteCloudConfig()
    config.timeout = 99
    config.memory = True

    sqlitecloud.connect(account, config)

    mock_connect.assert_called_once_with(account.hostname, account.port, config)


def test_connect_with_connection_string_and_parameters(mocker: MockerFixture):
    mock_connect = mocker.patch("sqlitecloud.driver.Driver.connect")

    sqlitecloud.connect(
        "sqlitecloud://user:pass@myhost:1234/dbname?timeout=99&memory=true"
    )

    mock_connect.assert_called_once()
    assert mock_connect.call_args[0][0] == "myhost"
    assert mock_connect.call_args[0][1] == 1234
    # config
    assert mock_connect.call_args[0][2].timeout == 99
    assert mock_connect.call_args[0][2].memory


class TestCursor:
    def test_description_empty(self, mocker):
        cursor = Cursor(mocker.patch("sqlitecloud.Connection"))

        description = cursor.description

        assert description is None

    def test_description_with_resultset(self, mocker):
        cursor = Cursor(mocker.patch("sqlitecloud.Connection"))

        result = SQLiteCloudResult(SQLITECLOUD_RESULT_TYPE.RESULT_ROWSET)
        result.ncols = 1
        result.nrows = 1
        result.data = ["myname"]
        result.colname = ["column1"]
        cursor._resultset = result

        assert cursor.description == (("column1", None, None, None, None, None, None),)

    def test_description_with_resultset_multiple_rows(self, mocker):
        cursor = Cursor(mocker.patch("sqlitecloud.Connection"))

        result = SQLiteCloudResult(SQLITECLOUD_RESULT_TYPE.RESULT_ROWSET)
        result.ncols = 2
        result.nrows = 1
        result.data = ["myname"]
        result.colname = ["name", "id"]
        cursor._resultset = result

        assert cursor.description == (
            ("name", None, None, None, None, None, None),
            ("id", None, None, None, None, None, None),
        )

    def test_rowcount_with_rowset(self, mocker):
        cursor = Cursor(mocker.patch("sqlitecloud.Connection"))

        result = SQLiteCloudResult(SQLITECLOUD_RESULT_TYPE.RESULT_ROWSET)
        result.ncols = 1
        result.nrows = 3
        result.data = ["myname1", "myname2", "myname3"]
        result.colname = ["name"]
        cursor._resultset = result

        assert cursor.rowcount == 3

    def test_rowcount_with_result(self, mocker):
        cursor = Cursor(mocker.patch("sqlitecloud.Connection"))

        result = SQLiteCloudResult(SQLITECLOUD_RESULT_TYPE.RESULT_BLOB)
        cursor._resultset = result

        assert cursor.rowcount == -1

    def test_rowcount_with_no_resultset(self, mocker):
        cursor = Cursor(mocker.patch("sqlitecloud.Connection"))

        assert cursor.rowcount == -1

    def test_execute_escaped(self, mocker: MockerFixture):
        parameters = ("John's",)

        execute_mock = mocker.patch.object(Driver, "execute")

        cursor = Cursor(mocker.patch("sqlitecloud.Connection"))
        apply_adapter_mock = mocker.patch.object(cursor, "_adapt_parameters")
        apply_adapter_mock.return_value = parameters

        sql = "SELECT * FROM users WHERE name = ?"

        cursor.execute(sql, parameters)

        assert (
            execute_mock.call_args[0][0] == "SELECT * FROM users WHERE name = 'John''s'"
        )

    def test_executemany(self, mocker):
        seq_of_parameters = [("John", 25), ("Jane", 30), ("Bob", 40)]

        cursor = Cursor(mocker.patch("sqlitecloud.Connection"))
        execute_mock = mocker.patch.object(cursor, "execute")
        apply_adapter_mock = mocker.patch.object(cursor, "_adapt_parameters")
        apply_adapter_mock.side_effect = seq_of_parameters

        sql = "INSERT INTO users (name, age) VALUES (?, ?)"

        cursor.executemany(sql, seq_of_parameters)

        execute_mock.assert_called_once_with(
            "INSERT INTO users (name, age) VALUES ('John', 25);INSERT INTO users (name, age) VALUES ('Jane', 30);INSERT INTO users (name, age) VALUES ('Bob', 40);"
        )

    def test_executemany_escaped(self, mocker):
        seq_of_parameters = [("O'Conner", 25)]

        cursor = Cursor(mocker.patch("sqlitecloud.Connection"))
        execute_mock = mocker.patch.object(cursor, "execute")
        apply_adapter_mock = mocker.patch.object(cursor, "_adapt_parameters")
        apply_adapter_mock.side_effect = seq_of_parameters

        sql = "INSERT INTO users (name, age) VALUES (?, ?)"

        cursor.executemany(sql, seq_of_parameters)

        execute_mock.assert_called_once_with(
            "INSERT INTO users (name, age) VALUES ('O''Conner', 25);"
        )

    def test_fetchone_with_no_resultset(self, mocker):
        cursor = Cursor(mocker.patch("sqlitecloud.Connection"))

        assert cursor.fetchone() is None

    def test_fetchone_with_result(self, mocker):
        cursor = Cursor(mocker.patch("sqlitecloud.Connection"))

        result = SQLiteCloudResult(SQLITECLOUD_RESULT_TYPE.RESULT_BLOB)
        cursor._resultset = result

        assert cursor.fetchone() is None

    def test_fetchone_with_rowset(self, mocker):
        connection = mocker.patch("sqlitecloud.Connection")
        connection.text_factory = str

        cursor = Cursor(connection)

        result = SQLiteCloudResult(SQLITECLOUD_RESULT_TYPE.RESULT_ROWSET)
        result.ncols = 1
        result.nrows = 1
        result.data = ["myname"]
        result.colname = ["name"]
        cursor._resultset = result

        assert cursor.fetchone() == ("myname",)

    def test_fetchone_twice(self, mocker):
        cursor = Cursor(mocker.patch("sqlitecloud.Connection"))

        result = SQLiteCloudResult(SQLITECLOUD_RESULT_TYPE.RESULT_ROWSET)
        result.ncols = 1
        result.nrows = 1
        result.data = ["myname"]
        result.colname = ["name"]
        cursor._resultset = result

        assert cursor.fetchone() is not None
        assert cursor.fetchone() is None

    def test_fetchmany_with_no_resultset(self, mocker):
        cursor = Cursor(mocker.patch("sqlitecloud.Connection"))

        assert cursor.fetchmany() == []

    def test_fetchmany_with_result(self, mocker):
        cursor = Cursor(mocker.patch("sqlitecloud.Connection"))

        result = SQLiteCloudResult(SQLITECLOUD_RESULT_TYPE.RESULT_BLOB)
        cursor._resultset = result

        assert cursor.fetchmany() == []

    def test_fetchmany_with_rowset_and_default_size(self, mocker):
        connection = mocker.patch("sqlitecloud.Connection")
        connection.text_factory = str

        cursor = Cursor(connection)

        result = SQLiteCloudResult(SQLITECLOUD_RESULT_TYPE.RESULT_ROWSET)
        result.ncols = 1
        result.nrows = 3
        result.data = ["myname1", "myname2", "myname3"]
        result.colname = ["name"]
        cursor._resultset = result

        assert cursor.fetchmany(None) == [("myname1",)]

    def test_fetchmany_twice_to_retrieve_whole_rowset(self, mocker):
        connection = mocker.patch("sqlitecloud.Connection")
        connection.text_factory = str

        cursor = Cursor(connection)

        result = SQLiteCloudResult(SQLITECLOUD_RESULT_TYPE.RESULT_ROWSET)
        result.ncols = 1
        result.nrows = 2
        result.data = ["myname1", "myname2"]
        result.colname = ["name"]
        cursor._resultset = result

        assert cursor.fetchmany(2) == [("myname1",), ("myname2",)]
        assert cursor.fetchmany() == []

    def test_fetchmany_with_size_higher_than_rowcount(self, mocker):
        connection = mocker.patch("sqlitecloud.Connection")
        connection.text_factory = str

        cursor = Cursor(connection)

        result = SQLiteCloudResult(SQLITECLOUD_RESULT_TYPE.RESULT_ROWSET)
        result.ncols = 1
        result.nrows = 1
        result.data = ["myname1"]
        result.colname = ["name"]
        cursor._resultset = result

        assert cursor.fetchmany(2) == [("myname1",)]

    def test_fetchall_with_no_resultset(self, mocker):
        cursor = Cursor(mocker.patch("sqlitecloud.Connection"))

        assert cursor.fetchall() == []

    def test_fetchall_with_result(self, mocker):
        cursor = Cursor(mocker.patch("sqlitecloud.Connection"))

        result = SQLiteCloudResult(SQLITECLOUD_RESULT_TYPE.RESULT_BLOB)
        cursor._resultset = result

        assert cursor.fetchall() == []

    def test_fetchall_with_rowset(self, mocker):
        connection = mocker.patch("sqlitecloud.Connection")
        connection.text_factory = str

        cursor = Cursor(connection)

        result = SQLiteCloudResult(SQLITECLOUD_RESULT_TYPE.RESULT_ROWSET)
        result.ncols = 1
        result.nrows = 3
        result.data = ["myname1", "myname2", "myname3"]
        result.colname = ["name"]
        cursor._resultset = result

        assert cursor.fetchall() == [("myname1",), ("myname2",), ("myname3",)]

    def test_fetchall_twice_and_expect_empty_list(self, mocker):
        connection = mocker.patch("sqlitecloud.Connection")
        connection.text_factory = str

        cursor = Cursor(connection)

        result = SQLiteCloudResult(SQLITECLOUD_RESULT_TYPE.RESULT_ROWSET)
        result.ncols = 1
        result.nrows = 2
        result.data = ["myname1", "myname2"]
        result.colname = ["name"]
        cursor._resultset = result

        assert cursor.fetchall() == [("myname1",), ("myname2",)]
        assert cursor.fetchall() == []

    def test_fetchall_to_return_remaining_rows(self, mocker):
        connection = mocker.patch("sqlitecloud.Connection")
        connection.text_factory = str

        cursor = Cursor(connection)

        result = SQLiteCloudResult(SQLITECLOUD_RESULT_TYPE.RESULT_ROWSET)
        result.ncols = 1
        result.nrows = 2
        result.data = ["myname1", "myname2"]
        result.colname = ["name"]
        cursor._resultset = result

        assert cursor.fetchone() is not None
        assert cursor.fetchall() == [("myname2",)]

    def test_iterator(self, mocker):
        connection = mocker.patch("sqlitecloud.Connection")
        connection.text_factory = str

        cursor = Cursor(connection)

        result = SQLiteCloudResult(SQLITECLOUD_RESULT_TYPE.RESULT_ROWSET)
        result.ncols = 1
        result.nrows = 2
        result.data = ["myname1", "myname2"]
        result.colname = ["name"]
        cursor._resultset = result

        assert list(cursor) == [("myname1",), ("myname2",)]

    def test_row_factory(self, mocker):
        conn = Connection(mocker.patch("sqlitecloud.datatypes.SQLiteCloudConnect"))
        conn.row_factory = lambda x, y: {"name": y[0]}

        result = SQLiteCloudResult(SQLITECLOUD_RESULT_TYPE.RESULT_ROWSET)
        result.ncols = 1
        result.nrows = 2
        result.data = ["myname1", "myname2"]
        result.colname = ["name"]

        cursor = conn.cursor()
        cursor._resultset = result

        assert cursor.fetchone() == {"name": "myname1"}

    @pytest.mark.parametrize(
        "method, args",
        [
            ("execute", ("",)),
            ("executemany", ("", [])),
            ("fetchone", ()),
            ("fetchmany", ()),
            ("fetchall", ()),
            ("close", ()),
        ],
    )
    def test_close_raises_expected_exception_on_any_further_operation(
        self, method, args, mocker
    ):
        cursor = Cursor(mocker.patch("sqlitecloud.Connection"))

        cursor.close()

        with pytest.raises(SQLiteCloudException) as e:
            getattr(cursor, method)(*args)

        assert e.value.args[0] == "The cursor is closed."
