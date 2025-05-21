import pytest
from pytest_mock import MockerFixture

from sqlitecloud.datatypes import SQLiteCloudAccount, SQLiteCloudConfig
from sqlitecloud.driver import Driver


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
            (":5678 ", 5678, 0, 6),
            (":0 ", 0, 0, 3),
            (",3.14 ", 3.14, 0, 6),
            (",0 ", 0, 0, 3),
            (",0.0 ", 0.0, 0, 5),
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
        expected_list = ["Hello World", 123456, 3.1415, None, b"0123456789"]

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
            b"SET CLIENT KEY NONLINEARIZABLE TO 1;AUTH USER pippo PASSWORD pluto;"
        )

        run_command_mock.assert_called_once()
        assert expected_buffer in run_command_mock.call_args[0][1]

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

        expected_buffer = b"SET CLIENT KEY NONLINEARIZABLE TO 1;AUTH APIKEY abc123;"

        run_command_mock.assert_called_once()
        assert expected_buffer in run_command_mock.call_args[0][1]

    def test_compression_enabled_by_default(self, mocker: MockerFixture):
        driver = Driver()

        config = SQLiteCloudConfig()
        config.account = SQLiteCloudAccount()
        config.account.apikey = "abc123"

        mocker.patch.object(driver, "_internal_connect", return_value=None)
        run_command_mock = mocker.patch.object(driver, "_internal_run_command")

        driver.connect("myhost", 8860, config)

        expected_buffer = b"SET CLIENT KEY COMPRESSION TO 1;"

        run_command_mock.assert_called_once()
        assert expected_buffer in run_command_mock.call_args[0][1]

    @pytest.mark.parametrize(
        "data, expected, zero_string",
        [
            ("hello world", b"+11 hello world", False),
            ("hello world", b"!12 hello world\0", True),
            (123, b":123 ", False),
            (3.14, b",3.14 ", False),
            (b"hello", b"$5 hello", False),
            (None, b"_ ", False),
            (
                ["SELECT ?, ?, ?, ?, ?", "world", 123, 3.14, None, b"hello"],
                b"=57 6 !21 SELECT ?, ?, ?, ?, ?\x00!6 world\x00:123 ,3.14 _ $5 hello",
                True,
            ),
            (
                ["SELECT ?", "'hello world'"],
                b"=31 2 !9 SELECT ?\x00+13 'hello world'",
                False,
            ),
        ],
    )
    def test_internal_serialize_command(self, data, zero_string, expected):
        driver = Driver()

        serialized = driver._internal_serialize_command(data, zero_string=zero_string)

        assert serialized == expected

    def test_expect_command_auth_token(self, mocker: MockerFixture):
        driver = Driver()

        config = SQLiteCloudConfig()
        config.account = SQLiteCloudAccount()
        config.account.token = "abc123"

        mocker.patch.object(driver, "_internal_connect", return_value=None)
        run_command_mock = mocker.patch.object(driver, "_internal_run_command")

        driver.connect("myhost", 8860, config)

        expected_buffer = b"AUTH TOKEN abc123;"

        run_command_mock.assert_called_once()
        assert expected_buffer in run_command_mock.call_args[0][1]
        assert b"AUTH APIKEY" not in run_command_mock.call_args[0][1]
