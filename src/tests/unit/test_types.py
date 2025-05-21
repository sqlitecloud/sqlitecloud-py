import pytest

from sqlitecloud.datatypes import SQLiteCloudConfig
from sqlitecloud.exceptions import SQLiteCloudException


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

    def test_parse_connection_string_error_with_both_user_pass_and_apikey(self):
        connection_string = "sqlitecloud://user:password@host:1234/database?apikey=xxx"

        with pytest.raises(SQLiteCloudException, match="Invalid connection string"):
            SQLiteCloudConfig(connection_string)

    def test_parse_connection_string_error_with_both_user_pass_and_token(self):
        connection_string = "sqlitecloud://user:password@host:1234/database?token=xxx"

        with pytest.raises(SQLiteCloudException, match="Invalid connection string"):
            SQLiteCloudConfig(connection_string)

    def test_parse_connection_string_error_with_both_apikey_and_token(self):
        connection_string = "sqlitecloud://host:1234/database?apikey=xxx&token=yyy"

        with pytest.raises(SQLiteCloudException, match="Invalid connection string"):
            SQLiteCloudConfig(connection_string)
