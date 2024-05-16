from sqlitecloud.driver import Driver
import pytest


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