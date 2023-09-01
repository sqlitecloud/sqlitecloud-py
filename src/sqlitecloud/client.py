from typing import Tuple
from uuid import UUID, uuid4
from sqlitecloud.wrapper_types import SQCloudConfig
from sqlitecloud.driver import SQCloudConnect, SQCloudErrorMsg, SQCloudIsError


class SqliteCloudResultSet:
    pass


def _decode_conn_str(connection_str: str) -> Tuple[str, str, str, int]:
    return "user", "pass", "host", 8860


class SqliteCloudClient:
    """_summary_"""

    # TODO connection pooling

    id: UUID

    def __init__(self, connection_str: str, uuid: UUID = uuid4()) -> None:
        self.id = uuid
        self.username, self.password, self.hostname, self.port = _decode_conn_str(
            connection_str
        )

    def _open_connection(self) -> SQCloudConnect:
        config = SQCloudConfig()
        config.username = self.username
        config.password = self.password
        # Set other config properties...
        connection = SQCloudConnect(self.hostname, self.port, config)
        is_error = SQCloudIsError(connection)
        if is_error:  # TODO error handling
            error_message = SQCloudErrorMsg(connection)
            print("An error occurred.", error_message.decode("utf-8"))
        else:
            return connection

    # Use the connection to interact with the SQCloud API...

    def _close_connection(self) -> None:
        pass

    def exec_query(self, query: str) -> SqliteCloudResultSet:
        conn = self._open_connection()
