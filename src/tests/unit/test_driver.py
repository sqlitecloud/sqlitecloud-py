import pytest
from pytest_mock import MockerFixture

from sqlitecloud.driver import Driver
from sqlitecloud.types import SQLiteCloudAccount, SQLiteCloudConfig


class TestDriver:
    @pytest.fixture(
        params=[
            (":0 ", 0, 0, 3),
            (":123 ", 123, 0, 5),
            (",123.456 ", 1230456, 0, 9),
            ("-1:1234 ", 1, 1234, 8),
            ("-0:0 ", 0, 0, 5),
            ("-123:456 ", 123, 456, 9),
            ("-123: ", 123, 0, 6),
            ("-1234:5678 ", 1234, 5678, 11),
            ("-1234: ", 1234, 0, 7),
        ]
    )
    def number_data(self, request):
        return request.param

    def test_parse_number(self, number_data):
        driver = Driver()
        buffer, expected_value, expected_extcode, expected_cstart = number_data
        result = driver._internal_parse_number(buffer.encode())

        assert expected_value == result.value
        assert expected_extcode == result.extcode
        assert expected_cstart == result.cstart

    @pytest.fixture(
        params=[
            ("+5 Hello", "Hello", 5, 8),
            ("+11 Hello World", "Hello World", 11, 15),
            ("!6 Hello0", "Hello", 5, 9),
            ("+0 ", "", 0, 3),
            (":5678 ", "5678", 0, 6),
            (":0 ", "0", 0, 3),
            (",3.14 ", "3.14", 0, 6),
            (",0 ", "0", 0, 3),
            (",0.0 ", "0.0", 0, 5),
            ("_ ", None, 0, 2),
        ],
        ids=[
            "String",
            "String with space",
            "String zero-terminated",
            "Empty string",
            "Integer",
            "Integer zero",
            "Float",
            "Float zero",
            "Float 0.0",
            "Null",
        ],
    )
    def value_data(self, request):
        return request.param

    def test_parse_value(self, value_data):
        driver = Driver()
        buffer, expected_value, expected_len, expected_cellsize = value_data

        result = driver._internal_parse_value(buffer.encode())

        assert expected_value == result.value
        assert expected_len == result.len
        assert expected_cellsize == result.cellsize

    def test_parse_array(self):
        driver = Driver()
        buffer = b"=5 +11 Hello World:123456 ,3.1415 _ $10 0123456789"
        expected_list = ["Hello World", "123456", "3.1415", None, "0123456789"]

        result = driver._internal_parse_array(buffer)

        assert expected_list == result

    def test_parse_rowset_signature(self):
        driver = Driver()
        buffer = b"*35 0:1 1 2 +2 42+7 'hello':42 +5 hello"

        result = driver._internal_parse_rowset_signature(buffer)

        assert 12 == result.start
        assert 35 == result.len
        assert 0 == result.idx
        assert 1 == result.version
        assert 1 == result.nrows
        assert 2 == result.ncols

    def test_prepare_statement_with_tuple_parameters(self):
        driver = Driver()

        query = "SELECT * FROM users WHERE age > ? AND name = ?"
        parameters = (18, "John")

        expected_result = "SELECT * FROM users WHERE age > 18 AND name = 'John'"
        result = driver.prepare_statement(query, parameters)

        assert expected_result == result

    def test_prepare_statement_with_dict_parameters(self):
        driver = Driver()

        query = "INSERT INTO users (name, age) VALUES (:name, :age)"
        parameters = {"name": "Alice", "age": 25}

        expected_result = "INSERT INTO users (name, age) VALUES ('Alice', 25)"
        result = driver.prepare_statement(query, parameters)

        assert expected_result == result

    def test_prepare_statement_with_missing_parameters_does_not_raise_exception(self):
        driver = Driver()

        query = "UPDATE users SET name = :name, age = :age WHERE id = :id"
        parameters = {"name": "Bob"}

        expected_result = "UPDATE users SET name = 'Bob', age = :age WHERE id = :id"

        result = driver.prepare_statement(query, parameters)

        assert expected_result == result

    def test_prepare_statement_with_extra_parameters(self):
        driver = Driver()

        query = "SELECT * FROM users WHERE age > :age"
        parameters = {"age": 30, "name": "Alice"}

        expected_result = "SELECT * FROM users WHERE age > 30"

        result = driver.prepare_statement(query, parameters)

        assert expected_result == result

    def test_prepare_statement_with_sql_injection_threat(self):
        driver = Driver()

        query = "SELECT * FROM phone WHERE name = ?"
        parameter = ("Jack's phone; DROP TABLE phone;",)

        expected_result = (
            "SELECT * FROM phone WHERE name = 'Jack''s phone; DROP TABLE phone;'"
        )
        result = driver.prepare_statement(query, parameter)

        assert expected_result == result

    def test_escape_sql_parameter_with_string(self):
        driver = Driver()
        param = "John's SQL"

        expected_result = "'John''s SQL'"
        result = driver.escape_sql_parameter(param)

        assert expected_result == result

    def test_escape_sql_parameter_with_integer(self):
        driver = Driver()
        param = 123

        expected_result = "123"
        result = driver.escape_sql_parameter(param)

        assert expected_result == result

    def test_escape_sql_parameter_with_float(self):
        driver = Driver()
        param = 3.14

        expected_result = "3.14"
        result = driver.escape_sql_parameter(param)

        assert expected_result == result

    def test_escape_sql_parameter_with_none(self):
        driver = Driver()
        param = None

        expected_result = "NULL"
        result = driver.escape_sql_parameter(param)

        assert expected_result == result

    def test_escape_sql_parameter_with_bool(self):
        driver = Driver()
        param = True

        expected_result = "1"
        result = driver.escape_sql_parameter(param)

        assert expected_result == result

    def test_escape_sql_parameter_with_bytes(self):
        driver = Driver()
        param = b"Hello"

        expected_result = "X'48656c6c6f'"
        result = driver.escape_sql_parameter(param)

        assert expected_result == result

    def test_escape_sql_parameter_with_dict(self):
        driver = Driver()
        param = {"name": "O'Conner", "age": 25}

        expected_result = '\'{"name": "O\'\'Conner", "age": 25}\''
        driver.escape_sql_parameter(param)

        assert expected_result

    def test_nonlinearizable_command_before_auth_with_account(
        self, mocker: MockerFixture
    ):
        driver = Driver()

        config = SQLiteCloudConfig()
        config.account = SQLiteCloudAccount()
        config.account.username = "pippo"
        config.account.password = "pluto"
        config.non_linearizable = True

        mocker.patch.object(driver, "_internal_connect", return_value=None)
        run_command_mock = mocker.patch.object(driver, "_internal_run_command")

        driver.connect("myhost", 8860, config)

        expected_buffer = (
            "SET CLIENT KEY NONLINEARIZABLE TO 1;AUTH USER pippo PASSWORD pluto;"
        )

        run_command_mock.assert_called_once()
        assert run_command_mock.call_args[0][1] == expected_buffer

    def test_nonlinearizable_command_before_auth_with_apikey(
        self, mocker: MockerFixture
    ):
        driver = Driver()

        config = SQLiteCloudConfig()
        config.account = SQLiteCloudAccount()
        config.account.apikey = "abc123"
        config.non_linearizable = True

        mocker.patch.object(driver, "_internal_connect", return_value=None)
        run_command_mock = mocker.patch.object(driver, "_internal_run_command")

        driver.connect("myhost", 8860, config)

        expected_buffer = "SET CLIENT KEY NONLINEARIZABLE TO 1;AUTH APIKEY abc123;"

        run_command_mock.assert_called_once()
        assert run_command_mock.call_args[0][1] == expected_buffer
