""" Module to interact with remote SqliteCloud database

"""
from typing import Tuple
from uuid import UUID, uuid4
from sqlitecloud.wrapper_types import SQCloudConfig
from sqlitecloud.driver import SQCloudConnect, SQCloudErrorMsg, SQCloudIsError


class SqliteCloudResultSet:
    pass


def _decode_conn_str(connection_str: str) -> Tuple[str, str, str, int]:
    print(connection_str)
    return b"user", b"pass", b"host", 8860


class SqliteCloudClient:
    """_summary_"""

    # TODO connection pooling

    id: UUID

    def __init__(self, connection_str: str, uuid: UUID = uuid4()) -> None:
        """Initializes a new instance of the class.

        Args:
            connection_str (str): The connection string for the database.
            uuid (UUID, optional): The UUID for the instance. Defaults to a new UUID generated using uuid4().

        Raises:
            ValueError: If the connection string is invalid.

        Returns:
            None
        """
        self.u_id = uuid
        self.username, self.password, self.hostname, self.port = _decode_conn_str(
            connection_str
        )
        print(self.username)

    def _open_connection(self) -> SQCloudConnect:
        """Opens a connection to the SQCloud server.

        Returns:
            SQCloudConnect: An instance of the SQCloudConnect class representing the connection to the SQCloud server.

        Raises:
            Exception: If an error occurs while opening the connection.
        """
        config = SQCloudConfig()
        config.username = self.username
        config.password = self.password
        # Set other config properties...
        connection = SQCloudConnect(self.hostname, self.port, config)
        is_error = SQCloudIsError(connection)
        if is_error:  # TODO error handling
            error_message = SQCloudErrorMsg(connection)
            print("An error occurred.", error_message.decode("utf-8"))
            raise Exception(error_message)

        return connection

    def _close_connection(self) -> None:
        """Closes the connection to the database.

        This method is used to close the connection to the database. It does not take any arguments and does not return any value.

        Returns:
            None: This method does not return any value.
        """

    def exec_query(self, query: str) -> SqliteCloudResultSet:
        """Executes a SQL query on the SQLite database.

        Args:
            query (str): The SQL query to be executed.

        Returns:
            SqliteCloudResultSet: The result set of the executed query.
        """
        conn = self._open_connection()
        print(query, dir(conn))
        return lambda a: "None " + a
