import pytest

from sqlitecloud.types import SQLiteCloudConfig


class TestSQLiteCloudConfig:
    @pytest.mark.parametrize(
        "param, value",
        [
            ("non_linearizable", True),
            ("nonlinearizable", True),
        ],
    )
    def test_parse_connection_string_with_nonlinarizable(self, param: str, value: any):
        connection_string = f"sqlitecloud://myhost.sqlitecloud.io?{param}={value}"

        config = SQLiteCloudConfig(connection_string)

        assert config.non_linearizable
