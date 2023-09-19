""" Module to interact with remote SqliteCloud database

"""
import ctypes
from dataclasses import dataclass
from typing import Optional
from sqlitecloud.resultset import SqliteCloudResultSet
from sqlitecloud.wrapper_types import SQCloudConfig, SQCloudResult
from sqlitecloud.driver import (
    SQCloudConnect,
    SQCloudErrorMsg,
    SQCloudIsError,
    SQCloudExec,
    SQCloudConnectWithString,
    SQCloudDisconnect,
)


@dataclass
class SqliteCloudAccount:
    username: str
    password: str
    hostname: str
    dbname: str
    port: int


class SqliteCloudClient:
    """_summary_"""

    # TODO connection pooling
    _config: SQCloudConfig = (None,)
    connection_str: str = None
    hostname: str
    dbname: str
    port: int

    def __init__(
        self,
        cloud_account: Optional[SqliteCloudAccount] = None,
        connection_str: Optional[str] = None,
    ) -> None:
        """Initializes a new instance of the class.

        Args:
            connection_str (str): The connection string for the database.
            uuid (UUID, optional): The UUID for the instance. Defaults to a new UUID generated using uuid4().

        Raises:
            ValueError: If the connection string is invalid.

        """

        if connection_str:
            print(connection_str)
            self.connection_str = connection_str
        elif cloud_account:
            self.config = SQCloudConfig()
            self.config.username = self.encode_str_to_c(cloud_account.username)
            self.config.password = self.encode_str_to_c(cloud_account.password)
            self.hostname = cloud_account.hostname
            self.dbname = cloud_account.dbname
            self.port = cloud_account.port

        else:
            raise Exception("Missing connection parameters")

    def encode_str_to_c(self, username):
        return ctypes.c_char_p(username.encode("utf-8"))

    def open_connection(self) -> SQCloudConnect:
        """Opens a connection to the SQCloud server.

        Returns:
            SQCloudConnect: An instance of the SQCloudConnect class representing the connection to the SQCloud server.

        Raises:
            Exception: If an error occurs while opening the connection.
        """

        # Set other config properties...
        connection = None
        if self.connection_str:
            connection = SQCloudConnectWithString(self.connection_str, None)
        else:
            connection = SQCloudConnect(
                self.encode_str_to_c(self.hostname), self.port, self.config
            )
        self._check_connection(connection)
        SQCloudExec(connection, self.encode_str_to_c(f"USE DATABASE {self.dbname};"))
        self._check_connection(connection)

        return connection

    def _check_connection(self, connection):
        is_error = SQCloudIsError(connection)
        if is_error:
            error_message = SQCloudErrorMsg(connection)
            print("An error occurred.", error_message.decode("utf-8"))
            raise Exception(error_message)

    def disconnect(self, conn: SQCloudConnect) -> None:
        """Closes the connection to the database.

        This method is used to close the connection to the database. It does not take any arguments and does not return any value.

        Returns:
            None: This method does not return any value.
        """
        SQCloudDisconnect(conn)

    def exec_query(
        self, query: str, conn: SQCloudConnect = None
    ) -> SqliteCloudResultSet:
        """Executes a SQL query on the SQLite database.

        Args:
            query (str): The SQL query to be executed.

        Returns:
            SqliteCloudResultSet: The result set of the executed query.
        """
        local_conn = conn if conn else self.open_connection()
        result: SQCloudResult = SQCloudExec(local_conn, self.encode_str_to_c(query))
        self._check_connection(local_conn)

        return SqliteCloudResultSet(result)
