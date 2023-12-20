""" Module to interact with remote SqliteCloud database

"""
import ctypes
from dataclasses import dataclass
from typing import Any, List, Optional, Tuple

from sqlitecloud.driver import (
    SQCloudConnect,
    SQCloudErrorMsg,
    SQCloudIsError,
    SQCloudExec,
    SQCloudExecArray,
    SQCloudConnectWithString,
    SQCloudDisconnect,
    SQCloudPubSubCB,
    SQCloudResultDump,
    SQCloudResultIsError,
    SqlParameter,
)
from sqlitecloud.pubsub import SQCloudPubSubCallback, subscribe_pub_sub
from sqlitecloud.resultset import SqliteCloudResultSet
from sqlitecloud.wrapper_types import SQCloudConfig, SQCloudResult


@dataclass
class SqliteCloudAccount:
    username: str
    password: str
    hostname: str
    dbname: str
    port: int


class SqliteCloudClient:
    """
    Client to connect to SqliteCloud
    """

    _config: Optional[SQCloudConfig] = None
    connection_str: Optional[str] = None
    hostname: str
    dbname: str
    port: int
    _pub_sub_cbs: List[Tuple[str, SQCloudPubSubCB]] = []

    def __init__(
        self,
        cloud_account: Optional[SqliteCloudAccount] = None,
        connection_str: Optional[str] = None,
        pub_subs: SQCloudPubSubCallback = [],
    ) -> None:
        """Initializes a new instance of the class.

        Args:
            connection_str (str): The connection string for the database.
            uuid (UUID, optional): The UUID for the instance. Defaults to a new UUID generated using uuid4().

        Raises:
            ValueError: If the connection string is invalid.

        """
        for pb in pub_subs:
            self._pub_sub_cbs.append(("channel1", SQCloudPubSubCB(pb)))
        if connection_str:
            self.connection_str = connection_str
        elif cloud_account:
            self.config = SQCloudConfig()
            self.config.username = self._encode_str_to_c(cloud_account.username)
            self.config.password = self._encode_str_to_c(cloud_account.password)
            self.hostname = cloud_account.hostname
            self.dbname = cloud_account.dbname
            self.port = cloud_account.port

        else:
            raise Exception("Missing connection parameters")

    def _encode_str_to_c(self, text):
        return ctypes.c_char_p(text.encode("utf-8"))

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
                self._encode_str_to_c(self.hostname), self.port, self.config
            )
        self._check_connection(connection)
        SQCloudExec(connection, self._encode_str_to_c(f"USE DATABASE {self.dbname};"))
        self._check_connection(connection)
        for cb in self._pub_sub_cbs:
            subscribe_pub_sub(connection, cb[0], cb[1])

        return connection

    def _check_connection(self, connection) -> None:
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
        print(query)
        # pylint: disable=unused-variable
        local_conn, close_at_end = (
            (conn, False) if conn else (self.open_connection(), True)
        )
        result: SQCloudResult = SQCloudExec(local_conn, self._encode_str_to_c(query))
        self._check_connection(local_conn)
        return SqliteCloudResultSet(result)

    def exec_statement(
        self, query: str, values: List[Any], conn: SQCloudConnect = None
    ) -> SqliteCloudResultSet:
        local_conn = conn if conn else self.open_connection()
        result: SQCloudResult = SQCloudExecArray(
            local_conn,
            self._encode_str_to_c(query),
            [SqlParameter(self._encode_str_to_c(str(v)), v) for v in values],
        )
        if SQCloudResultIsError(result):
            raise Exception(
                "Query error: " + str(SQCloudResultDump(local_conn, result))
            )
        return SqliteCloudResultSet(result)
