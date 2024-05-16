""" Module to interact with remote SqliteCloud database

"""
from typing import Optional
from urllib import parse

from sqlitecloud.driver import Driver
from sqlitecloud.resultset import SqliteCloudResultSet
from sqlitecloud.types import (
    SQCloudConfig,
    SQCloudConnect,
    SQCloudException,
    SqliteCloudAccount,
)


class SqliteCloudClient:
    """
    Client to interact with Sqlite Cloud
    """

    SQLITE_DEFAULT_PORT = 8860

    def __init__(
        self,
        cloud_account: Optional[SqliteCloudAccount] = None,
        connection_str: Optional[str] = None,
        # pub_subs: SQCloudPubSubCallback = [],
    ) -> None:
        """Initializes a new instance of the class with connection information.

        Args:
            connection_str (str): The connection string for the database.

        """
        self.driver = Driver()

        self.config = SQCloudConfig()

        if connection_str:
            self.config = self._parse_connection_string(connection_str)
        elif cloud_account:
            self.config.account = cloud_account
        else:
            raise SQCloudException("Missing connection parameters")

    def open_connection(self) -> SQCloudConnect:
        """Opens a connection to the SQCloud server.

        Returns:
            SQCloudConnect: An instance of the SQCloudConnect class representing the connection to the SQCloud server.

        Raises:
            SQCloudException: If an error occurs while opening the connection.
        """
        connection = self.driver.connect(
            self.config.account.hostname, self.config.account.port, self.config
        )

        return connection

    def disconnect(self, conn: SQCloudConnect) -> None:
        """Close the connection to the database."""
        self.driver.disconnect(conn)

    def exec_query(
        self, query: str, conn: SQCloudConnect = None
    ) -> SqliteCloudResultSet:
        """Executes a SQL query on the SQLite Cloud database.

        Args:
            query (str): The SQL query to be executed.

        Returns:
            SqliteCloudResultSet: The result set of the executed query.
        """
        provided_connection = conn is not None
        if not provided_connection:
            conn = self.open_connection()

        result = self.driver.execute(query, conn)

        if not provided_connection:
            self.disconnect(conn)

        return SqliteCloudResultSet(result)

    def sendblob(self, blob: bytes, conn: SQCloudConnect) -> SqliteCloudResultSet:
        """Sends a blob to the SQLite database.

        Args:
            blob (bytes): The blob to be sent to the database.
            conn (SQCloudConnect): The connection to the database.
        """
        return self.driver.sendblob(blob, conn)

    def _parse_connection_string(self, connection_string) -> SQCloudConfig:
        # URL STRING FORMAT
        # sqlitecloud://user:pass@host.com:port/dbname?timeout=10&key2=value2&key3=value3
        # or sqlitecloud://host.sqlite.cloud:8860/dbname?apikey=zIiAARzKm9XBVllbAzkB1wqrgijJ3Gx0X5z1A4m4xBA

        config = SQCloudConfig()
        config.account = SqliteCloudAccount()

        try:
            params = parse.urlparse(connection_string)

            options = {}
            query = params.query
            options = parse.parse_qs(query)
            for option, values in options.items():
                opt = option.lower()
                value = values.pop()

                if value.lower() in ["true", "false"]:
                    value = bool(value)
                elif value.isdigit():
                    value = int(value)
                else:
                    value = value

                if hasattr(config, opt):
                    setattr(config, opt, value)
                elif hasattr(config.account, opt):
                    setattr(config.account, opt, value)

            # apikey or username/password is accepted
            if not config.account.apikey:
                config.account.username = (
                    parse.unquote(params.username) if params.username else ""
                )
                config.account.password = (
                    parse.unquote(params.password) if params.password else ""
                )

            path = params.path
            database = path.strip("/")
            if database:
                config.account.dbname = database

            config.account.hostname = params.hostname
            config.account.port = (
                int(params.port) if params.port else self.SQLITE_DEFAULT_PORT
            )

            return config
        except Exception as e:
            raise SQCloudException(
                f"Invalid connection string {connection_string}"
            ) from e
