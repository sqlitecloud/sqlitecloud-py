""" Module to interact with remote SqliteCloud database

"""
from typing import Any, List, Optional

from sqlitecloud.driver import Driver
from sqlitecloud.types import SQCloudConfig, SQCloudConnect, SqliteCloudAccount


class SqliteCloudClient:
    """
    Client to connect to SqliteCloud
    """

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
        
        self.hostname: str = ''
        self.port: int = 8860
        
        self.config = SQCloudConfig()

        # for pb in pub_subs:
        #     self._pub_sub_cbs.append(("channel1", SQCloudPubSubCB(pb)))
        if connection_str:
            # TODO: parse connection string to create the config
            self.config = SQCloudConfig()
        elif cloud_account:
            self.config.account = cloud_account
            self.hostname = cloud_account.hostname
            self.port = cloud_account.port

        else:
            raise Exception("Missing connection parameters")

    def open_connection(self) -> SQCloudConnect:
        """Opens a connection to the SQCloud server.

        Returns:
            SQCloudConnect: An instance of the SQCloudConnect class representing the connection to the SQCloud server.

        Raises:
            Exception: If an error occurs while opening the connection.
        """
        connection = self.driver.connect(self.hostname, self.port, self.config)

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

    # def exec_query(
    #     self, query: str, conn: SQCloudConnect = None
    # ) -> SqliteCloudResultSet:
        """Executes a SQL query on the SQLite database.

        Args:
            query (str): The SQL query to be executed.

        Returns:
            SqliteCloudResultSet: The result set of the executed query.
        """
        # print(query)
        # # pylint: disable=unused-variable
        # local_conn, close_at_end = (
        #     (conn, False) if conn else (self.open_connection(), True)
        # )
        # result: SQCloudResult = SQCloudExec(local_conn, self._encode_str_to_c(query))
        # self._check_connection(local_conn)
        # return SqliteCloudResultSet(result)
        # pass

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
