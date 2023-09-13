""" Module to interact with remote SqliteCloud database

"""
import ctypes
from typing import Dict, Optional, Tuple
from uuid import UUID, uuid4
from sqlitecloud.wrapper_types import SQCloudConfig,SQCloudResult
from sqlitecloud.driver import SQCloudConnect, SQCloudErrorMsg, SQCloudIsError,SQCloudExec, SQCloudConnectWithString



class SqliteCloudResultSet:
    _result = SQCloudResult

    def __init__(self,result:SQCloudResult) -> None:
        self._result = result

    def __iter__(self):
        for row in range(self._result.contents.num_rows):
            out: Dict[str,any] # todo convert type
            for col in range(self._result.contents.num_columns):
                data = self._result.contents.data[row * self._result.contents.num_columns + col].decode("utf-8")
                column_name = self._result.contents.column_names[col].decode("utf-8")
                out[column_name] = data
            yield out


class SqliteCloudClient:
    """_summary_"""

    # TODO connection pooling

    id: UUID
    _config:SQCloudConfig
    connection_str:str = None
    hostname:str
    port:int

    def __init__(self, username:Optional[str]=None, password:Optional[str]=None, hostname:Optional[str]=None, port:Optional[int]=None ,  connection_str: Optional[str]=None) -> None:
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
        elif username and password:
            self.config = SQCloudConfig()
            self.config.username = self.encode_str_to_c(username)
            self.config.password =  self.encode_str_to_c(password)
            self.hostname = hostname
            self.port = port
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
            connection = SQCloudConnectWithString(self.connection_str,None)
        else:
            connection = SQCloudConnect(self.encode_str_to_c(self.hostname), self.port, self.config)
        is_error = SQCloudIsError(connection)
        if is_error:  # TODO error handling
            error_message = SQCloudErrorMsg(connection)
            print("An error occurred.", error_message.decode("utf-8"))
            raise Exception(error_message)

        return connection

    def close_connection(self) -> None:
        """Closes the connection to the database.

        This method is used to close the connection to the database. It does not take any arguments and does not return any value.

        Returns:
            None: This method does not return any value.
        """

    def exec_query(self, query: str, conn:SQCloudConnect=None) -> SqliteCloudResultSet:
        """Executes a SQL query on the SQLite database.

        Args:
            query (str): The SQL query to be executed.

        Returns:
            SqliteCloudResultSet: The result set of the executed query.
        """
        local_conn = conn if conn else self.open_connection()
        result = SQCloudExec(local_conn, ctypes.byref(query))
        return SqliteCloudResultSet(result)
