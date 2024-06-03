# DB-API 2.0 interface to SQLite Cloud.
#
# PEP 249 – Python Database API Specification v2.0
# https://peps.python.org/pep-0249/
#
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Tuple,
    Union,
    overload,
)

from sqlitecloud.driver import Driver
from sqlitecloud.resultset import SQLiteCloudResult
from sqlitecloud.types import (
    SQLITECLOUD_RESULT_TYPE,
    SQLiteCloudAccount,
    SQLiteCloudConfig,
    SQLiteCloudConnect,
    SQLiteCloudDataTypes,
    SQLiteCloudException,
)

# Question mark style, e.g. ...WHERE name=?
# Module also supports Named style, e.g. ...WHERE name=:name
paramstyle = "qmark"

# Threads may share the module, but not connections
threadsafety = 1

# DB API level
apilevel = "2.0"


@overload
def connect(connection_str: str) -> "Connection":
    """
    Establishes a connection to the SQLite Cloud database.

    Args:
        connection_str (str): The connection string for the database.
            It may include SQLiteCloudConfig'options like timeout, apikey, etc. in the url query string.
            Eg: sqlitecloud://myhost.sqlitecloud.io:8860/mydb?apikey=abc123&compression=true&timeout=10

    Returns:
        Connection: A connection object representing the database connection.

    Raises:
        SQLiteCloudException: If an error occurs while establishing the connection.
    """
    ...


@overload
def connect(
    cloud_account: SQLiteCloudAccount, config: Optional[SQLiteCloudConfig] = None
) -> "Connection":
    """
    Establishes a connection to the SQLite Cloud database using the provided cloud account and configuration.

    Args:
        cloud_account (SqliteCloudAccount): The cloud account used to authenticate and access the database.
        config (Optional[SQLiteCloudConfig]): Additional configuration options for the connection (default: None).

    Returns:
        Connection: A connection object representing the connection to the SQLite Cloud database.

    Raises:
        SQLiteCloudException: If an error occurs while establishing the connection.
    """
    ...


def connect(
    connection_info: Union[str, SQLiteCloudAccount],
    config: Optional[SQLiteCloudConfig] = None,
) -> "Connection":
    """
    Establishes a connection to the SQLite Cloud database.

    Args:
        connection_info (Union[str, SqliteCloudAccount]): The connection information.
            It can be either a connection string or a `SqliteCloudAccount` object.
        config (Optional[SQLiteCloudConfig]): The configuration options for the connection.
            Defaults to None.

    Returns:
        Connection: A DB-API 2.0 connection object representing the connection to the database.

    Raises:
        SQLiteCloudException: If an error occurs while establishing the connection.
    """
    driver = Driver()

    if isinstance(connection_info, SQLiteCloudAccount):
        if not config:
            config = SQLiteCloudConfig()
        config.account = connection_info
    else:
        config = SQLiteCloudConfig(connection_info)

    return Connection(
        driver.connect(config.account.hostname, config.account.port, config)
    )


class Connection:
    """
    Represents a DB-APi 2.0 connection to the SQLite Cloud database.

    Args:
        SQLiteCloud_connection (SQLiteCloudConnect): The SQLite Cloud connection object.

    Attributes:
        _driver (Driver): The driver object used for database operations.
        SQLiteCloud_connection (SQLiteCloudConnect): The SQLite Cloud connection object.
    """

    row_factory: Optional[Callable[["Cursor", Tuple], object]] = None

    def __init__(self, SQLiteCloud_connection: SQLiteCloudConnect) -> None:
        self._driver = Driver()
        self.row_factory = None
        self.SQLiteCloud_connection = SQLiteCloud_connection

    @property
    def sqlcloud_connection(self) -> SQLiteCloudConnect:
        """
        Returns the SQLite Cloud connection object.

        Returns:
            SQLiteCloudConnect: The SQLite Cloud connection object.
        """
        return self.SQLiteCloud_connection

    def execute(
        self,
        sql: str,
        parameters: Union[
            Tuple[SQLiteCloudDataTypes], Dict[Union[str, int], SQLiteCloudDataTypes]
        ] = (),
    ) -> "Cursor":
        """
        Shortcut for cursor.execute().
        See the docstring of Cursor.execute() for more information.

        Args:
            sql (str): The SQL query to execute.
            parameters (Union[Tuple[SQLiteCloudDataTypes], Dict[Union[str, int], SQLiteCloudDataTypes]]):
                The parameters to be used in the query. It can be a tuple or a dictionary. (Default ())
            conn (SQLiteCloudConnect): The connection object to use for executing the query.

        Returns:
            Cursor: The cursor object.
        """
        cursor = self.cursor()
        return cursor.execute(sql, parameters)

    def executemany(
        self,
        sql: str,
        seq_of_parameters: Iterable[
            Union[
                Tuple[SQLiteCloudDataTypes], Dict[Union[str, int], SQLiteCloudDataTypes]
            ]
        ],
    ) -> "Cursor":
        """
        Shortcut for cursor.executemany().
        See the docstring of Cursor.executemany() for more information.

        Args:
            sql (str): The SQL statement to execute.
            seq_of_parameters (Iterable[Union[Tuple[SQLiteCloudDataTypes], Dict[Union[str, int], SQLiteCloudDataTypes]]]):
                The sequence of parameter sets to bind to the SQL statement.

        Returns:
            Cursor: The cursor object.
        """
        cursor = self.cursor()
        return cursor.executemany(sql, seq_of_parameters)

    def close(self):
        """
        Closes the database connection.
        All cursors created with this connection will become unusable after calling this method.

        Note:
            DB-API 2.0 interface does not manage the Sqlite Cloud PubSub feature.
            Therefore, only the main socket is closed.
        """
        self._driver.disconnect(self.SQLiteCloud_connection, True)

    def commit(self):
        """
        Not implementied yet.
        """

    def rollback(self):
        """
        Not implemented yet.
        """

    def cursor(self):
        """
        Creates a new cursor object.

        Returns:
            Cursor: The cursor object.
        """
        cursor = Cursor(self)
        cursor.row_factory = self.row_factory
        return cursor

    def __del__(self) -> None:
        self.close()


class Cursor(Iterator[Any]):
    """
    The DB-API 2.0 Cursor class represents a database cursor, which is used to interact with the database.
    It provides methods to execute SQL statements, fetch results, and manage the cursor state.

    Attributes:
        arraysize (int): The number of rows to fetch at a time with fetchmany(). Default is 1.
    """

    arraysize: int = 1

    row_factory: Optional[Callable[["Cursor", Tuple], object]] = None

    def __init__(self, connection: Connection) -> None:
        self._driver = Driver()
        self.row_factory = None
        self._connection = connection
        self._iter_row: int = 0
        self._resultset: SQLiteCloudResult = None

    @property
    def connection(self) -> Connection:
        """
        Returns the connection object associated with the database.

        Returns:
            Connection: The DB-API 2.0 connection object.
        """
        return self._connection

    @property
    def description(
        self,
    ) -> Optional[Tuple[Tuple[str, None, None, None, None, None, None], ...]]:
        """
        Each sequence contains information describing one result column.
        Only the first value of the tuple is set which represents the column name.
        """
        if not self._is_result_rowset():
            return None

        description = ()
        for i in range(self._resultset.ncols):
            description += (
                (
                    self._resultset.colname[i],
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                ),
            )

        return description

    @property
    def rowcount(self) -> int:
        """
        The number of rows that the last .execute*() produced (for DQL statements like SELECT)

        The number of rows affected by DML statements like UPDATE or INSERT is not supported.

        Returns:
            int: The number of rows in the result set or -1 if no result set is available.
        """
        return self._resultset.nrows if self._is_result_rowset() else -1

    @property
    def lastrowid(self) -> Optional[int]:
        """
        Not implemented yet in the library.
        """
        return None

    def close(self) -> None:
        """
        Just mark the cursors to be no more usable in SQLite Cloud database.
        In sqlite the `close()` is used to free up resources: https://devpress.csdn.net/python/62fe355b7e668234661931d8.html
        """
        self._ensure_connection()

        self._connection = None

    def execute(
        self,
        sql: str,
        parameters: Union[
            Tuple[SQLiteCloudDataTypes], Dict[Union[str, int], SQLiteCloudDataTypes]
        ] = (),
    ) -> "Cursor":
        """
        Prepare and execute a SQL statement (either a query or command) to the SQLite Cloud database.
        This function supports two styles of parameter markers:

        1. Question Mark Style: Parameters are passed as a tuple. For example:
        "SELECT * FROM table WHERE id = ?"

        2. Named Style: Parameters are passed as a dictionary. For example:
        "SELECT * FROM table WHERE id = :id"

        In both cases, the parameters replace the placeholders in the SQL statement.

        Shortcut for cursor.execute().

        Args:
            sql (str): The SQL query to execute.
            parameters (Union[Tuple[SQLiteCloudDataTypes], Dict[Union[str, int], SQLiteCloudDataTypes]]):
                The parameters to be used in the query. It can be a tuple or a dictionary. (Default ())
            conn (SQLiteCloudConnect): The connection object to use for executing the query.

        Returns:
            Cursor: The cursor object.
        """
        self._ensure_connection()

        prepared_statement = self._driver.prepare_statement(sql, parameters)
        result = self._driver.execute(
            prepared_statement, self.connection.sqlcloud_connection
        )

        self._resultset = result

        return self

    def executemany(
        self,
        sql: str,
        seq_of_parameters: Iterable[
            Union[
                Tuple[SQLiteCloudDataTypes], Dict[Union[str, int], SQLiteCloudDataTypes]
            ]
        ],
    ) -> "Cursor":
        """
        Executes a SQL statement multiple times, each with a different set of parameters.
        The entire statement is transmitted to the SQLite Cloud server in a single operation.
        This method is useful for executing the same query repeatedly with different values.

        Args:
            sql (str): The SQL statement to execute.
            seq_of_parameters (Iterable[Union[Tuple[SQLiteCloudDataTypes], Dict[Union[str, int], SQLiteCloudDataTypes]]]):
                The sequence of parameter sets to bind to the SQL statement.

        Returns:
            Cursor: The cursor object.
        """
        self._ensure_connection()

        commands = ""
        for parameters in seq_of_parameters:
            prepared_statement = self._driver.prepare_statement(sql, parameters)
            commands += prepared_statement + ";"

        self.execute(commands)

        return self

    def fetchone(self) -> Optional[Any]:
        """
        Fetches the next row of a result set, returning it as a single sequence,
        or None if no more rows are available.

        Returns:
            The next row of the query result set as a tuple,
                or None if no more rows are available.
        """
        self._ensure_connection()

        if not self._is_result_rowset():
            return None

        return next(self, None)

    def fetchmany(self, size=None) -> List[Any]:
        """
        Fetches the next set of rows from the result set.

        Args:
            size (int, optional): The maximum number of rows to fetch.
                If not specified, it uses the `arraysize` attribute.

        Returns:
            List[Tuple]: A list of rows, where each row is represented as a tuple.
        """
        self._ensure_connection()

        if not self._is_result_rowset():
            return []

        if size is None:
            size = self.arraysize

        results = []
        for _ in range(size):
            next_result = self.fetchone()
            if next_result is None:
                break
            results.append(next_result)

        return results

    def fetchall(self) -> List[Any]:
        """
        Fetches all remaining rows of a query result set.

        Returns:
            A list of rows, where each row is represented as a tuple.
        """
        self._ensure_connection()

        if not self._is_result_rowset():
            return []

        return self.fetchmany(self.rowcount)

    def setinputsizes(self, sizes) -> None:
        pass

    def setoutputsize(self, size, column=None) -> None:
        pass

    def _call_row_factory(self, row: Tuple) -> object:
        if self.row_factory is None:
            return row

        return self.row_factory(self, row)

    def _is_result_rowset(self) -> bool:
        return (
            self._resultset
            and self._resultset.tag == SQLITECLOUD_RESULT_TYPE.RESULT_ROWSET
        )

    def _ensure_connection(self):
        """
        Ensure the cursor is usable or has been closed.

        Raises:
            SQLiteCloudException: If the cursor is closed.
        """
        if not self._connection:
            raise SQLiteCloudException("The cursor is closed.")

    def __iter__(self) -> "Cursor":
        return self

    def __next__(self) -> Optional[Tuple[Any]]:
        self._ensure_connection()

        if (
            not self._resultset.is_result
            and self._resultset.data
            and self._iter_row < self._resultset.nrows
        ):
            out: tuple[Any] = ()

            for col in range(self._resultset.ncols):
                out += (self._resultset.get_value(self._iter_row, col),)
            self._iter_row += 1

            return self._call_row_factory(out)

        raise StopIteration
