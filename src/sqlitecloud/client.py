""" Module to interact with remote SqliteCloud database

"""
from typing import Any, List, Optional
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
    Client to connect to SqliteCloud
    """

    SQLITE_DEFAULT_PORT = 8860

    def __init__(
        self,
        cloud_account: Optional[SqliteCloudAccount] = None,
        connection_str: Optional[str] = None,
        # pub_subs: SQCloudPubSubCallback = [],
    ) -> None:
        """Initializes a new instance of the class.

        Args:
            connection_str (str): The connection string for the database.

        Raises:
            ValueError: If the connection string is invalid.

        """
        self.driver = Driver()

        self.config = SQCloudConfig()

        # for pb in pub_subs:
        #     self._pub_sub_cbs.append(("channel1", SQCloudPubSubCB(pb)))
        if connection_str:
            self.config = self._parse_connection_string(connection_str)
        elif cloud_account:
            self.config.account = cloud_account
        else:
            raise Exception("Missing connection parameters")

    def open_connection(self) -> SQCloudConnect:
        """Opens a connection to the SQCloud server.

        Returns:
            SQCloudConnect: An instance of the SQCloudConnect class representing the connection to the SQCloud server.

        Raises:
            Exception: If an error occurs while opening the connection.
        """
        connection = self.driver.connect(
            self.config.account.hostname, self.config.account.port, self.config
        )

        # SQCloudExec(connection, f"USE DATABASE {self.dbname};")

        # for cb in self._pub_sub_cbs:
        #     subscribe_pub_sub(connection, cb[0], cb[1])

        return connection

    def disconnect(self, conn: SQCloudConnect) -> None:
        """Closes the connection to the database.

        This method is used to close the connection to the database. It does not take any arguments and does not return any value.

        Returns:
            None: This method does not return any value.
        """
        self.driver.disconnect(conn)

    def exec_query(
        self, query: str, conn: SQCloudConnect = None
    ) -> SqliteCloudResultSet:
        """Executes a SQL query on the SQLite database.

        Args:
            query (str): The SQL query to be executed.

        Returns:
            SqliteCloudResultSet: The result set of the executed query.
        """
        if not conn:
            conn = self.open_connection()

        result = self.driver.execute(query, conn)
        return SqliteCloudResultSet(result)

    # def exec_statement(
    #     self, query: str, values: List[Any], conn: SQCloudConnect = None
    # ) -> SqliteCloudResultSet:
    # local_conn = conn if conn else self.open_connection()
    # result: SQCloudResult = SQCloudExecArray(
    #     local_conn,
    #     self._encode_str_to_c(query),
    #     [SqlParameter(self._encode_str_to_c(str(v)), v) for v in values],
    # )
    # if SQCloudResultIsError(result):
    #     raise Exception(
    #         "Query error: " + str(SQCloudResultDump(local_conn, result))
    #     )
    # return SqliteCloudResultSet(result)
    # pass

    def sendblob(self):
        pass

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
                config.account.database = database

            config.account.hostname = params.hostname
            config.account.port = (
                int(params.port) if params.port else self.SQLITE_DEFAULT_PORT
            )

            return config
        except Exception as e:
            raise SQCloudException(
                f"Invalid connection string {connection_string}"
            ) from e
