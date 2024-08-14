import pytest

from sqlitecloud.resultset import (
    SQLITECLOUD_RESULT_TYPE,
    SQLiteCloudResult,
    SQLiteCloudResultSet,
)


class TestSQLiteCloudResult:
    def test_init_data(self):
        result = SQLiteCloudResult(SQLITECLOUD_RESULT_TYPE.RESULT_INTEGER)
        result.init_data(42)
        assert 1 == result.nrows
        assert 1 == result.ncols
        assert [42] == result.data
        assert True is result.is_result

    def test_init_data_with_array(self):
        result = SQLiteCloudResult(SQLITECLOUD_RESULT_TYPE.RESULT_ARRAY)
        result.init_data([42, 43, 44])

        assert 1 == result.nrows
        assert 1 == result.ncols
        assert [[42, 43, 44]] == result.data
        assert True is result.is_result

    def test_init_as_dataset(self):
        result = SQLiteCloudResult(SQLITECLOUD_RESULT_TYPE.RESULT_ROWSET)

        assert False is result.is_result
        assert 0 == result.nrows
        assert 0 == result.ncols
        assert 0 == result.version

    def test_get_value_with_rowset(self):
        result = SQLiteCloudResult(SQLITECLOUD_RESULT_TYPE.RESULT_ROWSET)
        result.nrows = 2
        result.ncols = 2
        result.colname = ["name", "age"]
        result.data = ["John", 42, "Doe", 24]
        result.version = 2

        assert "John" == result.get_value(0, 0)
        assert 24 == result.get_value(1, 1)
        assert result.get_value(2, 2) is None

    def test_get_value_array(self):
        result = SQLiteCloudResult(
            SQLITECLOUD_RESULT_TYPE.RESULT_ARRAY, result=[1, 2, 3]
        )

        assert [1, 2, 3] == result.get_value(0, 0)

    def test_get_colname(self):
        result = SQLiteCloudResult(SQLITECLOUD_RESULT_TYPE.RESULT_ROWSET)
        result.ncols = 2
        result.colname = ["name", "age"]

        assert "name" == result.get_name(0)
        assert "age" == result.get_name(1)
        assert result.get_name(2) is None

    def test_get_value_with_empty_decltype(self):
        result = SQLiteCloudResult(SQLITECLOUD_RESULT_TYPE.RESULT_ROWSET)
        result.nrows = 2
        result.ncols = 2
        result.colname = []
        result.decltype = []
        result.data = ["John", "42", "Doe", "24"]

        assert "John" == result.get_value(0, 0)
        assert "42" == result.get_value(0, 1)
        assert "Doe" == result.get_value(1, 0)
        assert "24" == result.get_value(1, 1)

    def test_get_value_with_convert_false(self):
        result = SQLiteCloudResult(SQLITECLOUD_RESULT_TYPE.RESULT_ROWSET)
        result.nrows = 1
        result.ncols = 2
        result.colname = ["name", "age"]
        result.data = ["John", "42"]
        result.decltype = ["TEXT", "INTEGER"]

        assert "John" == result.get_value(0, 0)
        assert "42" == result.get_value(0, 1)


class TestSqliteCloudResultSet:
    def test_next(self):
        result = SQLiteCloudResult(SQLITECLOUD_RESULT_TYPE.RESULT_INTEGER, result=42)
        result_set = SQLiteCloudResultSet(result)

        assert {"result": 42} == next(result_set)
        with pytest.raises(StopIteration):
            next(result_set)

    def test_iter_result(self):
        result = SQLiteCloudResult(SQLITECLOUD_RESULT_TYPE.RESULT_INTEGER, result=42)
        result_set = SQLiteCloudResultSet(result)
        for row in result_set:
            assert {"result": 42} == row

    def test_iter_rowset(self):
        rowset = SQLiteCloudResult(SQLITECLOUD_RESULT_TYPE.RESULT_ROWSET)
        rowset.nrows = 2
        rowset.ncols = 2
        rowset.colname = ["name", "age"]
        rowset.data = ["John", 42, "Doe", 24]
        rowset.version = 2
        result_set = SQLiteCloudResultSet(rowset)

        out = []
        for row in result_set:
            out.append(row)

        assert 2 == len(out)
        assert {"name": "John", "age": 42} == out[0]
        assert {"name": "Doe", "age": 24} == out[1]

    def test_get_result_with_single_value(self):
        result = SQLiteCloudResult(SQLITECLOUD_RESULT_TYPE.RESULT_INTEGER, result=42)
        result_set = SQLiteCloudResultSet(result)

        assert 42 == result_set.get_result()
