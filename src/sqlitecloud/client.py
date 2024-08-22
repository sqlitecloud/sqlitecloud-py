""" Module to interact with remote SqliteCloud database

"""
from typing import Dict, Optional, Tuple, Union

from sqlitecloud.datatypes import (
    SQLiteCloudAccount,
    SQLiteCloudConfig,
    SQLiteCloudConnect,
    SQLiteCloudDataTypes,
)
from sqlitecloud.driver import Driver
from sqlitecloud.exceptions import SQLiteCloudException
from sqlitecloud.resultset import SQLiteCloudResultSet


class SQLiteCloudClient:
    """
    Client to interact with Sqlite Cloud
    """

    def __init__(
        self,
        cloud_account: Optional[SQLiteCloudAccount] = None,
        connection_str: Optional[str] = None,
    ) -> None:
        """Initializes a new instance of the class with connection information.

        Args:
            cloud_account (SqliteCloudAccount): The account information for the
                SQlite Cloud database.
            connection_str (str): The connection string for the SQlite Cloud database.
                Eg: sqlitecloud://user:pass@host.com:port/dbname?timeout=10&apikey=abcd123

        """
        self._driver = Driver()

        self.config = SQLiteCloudConfig()

        if connection_str:
            self.config = SQLiteCloudConfig(connection_str)
        elif cloud_account:
            self.config.account = cloud_account

        if self.config.account is None:
            raise SQLiteCloudException("Missing connection parameters")

    def open_connection(self) -> SQLiteCloudConnect:
        """Opens a connection to the SQLite Cloud server.

        Returns:
            SQLiteCloudConnect: An instance of the SQLiteCloudConnect class representing
                the connection to the SQLite Cloud server.

        Raises:
            SQLiteCloudException: If an error occurs while opening the connection.
        """
        connection = self._driver.connect(
            self.config.account.hostname, self.config.account.port, self.config
        )

        return connection

    def disconnect(self, conn: SQLiteCloudConnect) -> None:
        """Close the connection to the database."""
        self._driver.disconnect(conn)

    def is_connected(self, conn: SQLiteCloudConnect) -> bool:
        """Check if the connection is still open.

        Args:
            conn (SQLiteCloudConnect): The connection to the database.

        Returns:
            bool: True if the connection is open, False otherwise.
        """
        return self._driver.is_connected(conn)

    def exec_query(self, query: str, conn: SQLiteCloudConnect) -> SQLiteCloudResultSet:
        """Executes a SQL query on the SQLite Cloud database.

        Args:
            query (str): The SQL query to execute.

        Returns:
            SqliteCloudResultSet: The result set of the executed query.

        Raises:
            SQLiteCloudException: If an error occurs while executing the query.
        """
        result = self._driver.execute(query, conn)

        return SQLiteCloudResultSet(result)

    def exec_statement(
        self,
        query: str,
        parameters: Union[
            Tuple[SQLiteCloudDataTypes], Dict[Union[str, int], SQLiteCloudDataTypes]
        ],
        conn: SQLiteCloudConnect,
    ) -> SQLiteCloudResultSet:
        """
        Prepare and execute a SQL statement (either a query or command) to the SQLite Cloud database.
        This function supports two styles of parameter markers:

        1. Question Mark Style: Parameters are passed as a tuple. For example:
        "SELECT * FROM table WHERE id = ?"

        2. Named Style: Parameters are passed as a dictionary. For example:
        "SELECT * FROM table WHERE id = :id"

        In both cases, the parameters replace the placeholders in the SQL statement.

        Args:
            query (str): The SQL query to execute.
            parameters (Union[Tuple[SQLiteCloudDataTypes], Dict[Union[str, int], SQLiteCloudDataTypes]]):
                The parameters to be used in the query. It can be a tuple or a dictionary.
            conn (SQLiteCloudConnect): The connection object to use for executing the query.

        Returns:
            SqliteCloudResultSet: The result set obtained from executing the query.
        """
        result = self._driver.execute_statement(query, parameters, conn)

        return SQLiteCloudResultSet(result)

    def sendblob(self, blob: bytes, conn: SQLiteCloudConnect) -> SQLiteCloudResultSet:
        """Sends a blob to the SQLite database.

        Args:
            blob (bytes): The blob to be sent to the database.
            conn (SQLiteCloudConnect): The connection to the database.
        """
        return self._driver.send_blob(blob, conn)
