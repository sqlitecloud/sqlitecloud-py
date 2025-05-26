# DB-API 2.0 interface to SQLite Cloud.
#
# PEP 249 – Python Database API Specification v2.0
# https://peps.python.org/pep-0249/
#
import collections
import datetime
import logging
import re
import sys
import time
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Tuple,
    Type,
    Union,
    overload,
)

from sqlitecloud.datatypes import (
    SQLiteCloudAccount,
    SQLiteCloudConfig,
    SQLiteCloudConnect,
    SQLiteDataTypes,
)
from sqlitecloud.driver import Driver
from sqlitecloud.exceptions import (
    SQLiteCloudDatabaseError,
    SQLiteCloudDataError,
    SQLiteCloudError,
    SQLiteCloudIntegrityError,
    SQLiteCloudInterfaceError,
    SQLiteCloudInternalError,
    SQLiteCloudNotSupportedError,
    SQLiteCloudOperationalError,
    SQLiteCloudProgrammingError,
    SQLiteCloudWarning,
)
from sqlitecloud.resultset import (
    SQLITECLOUD_RESULT_TYPE,
    SQLITECLOUD_VALUE_TYPE,
    SQLiteCloudOperationResult,
    SQLiteCloudResult,
)

version = "0.1.0"
version_info = (0, 1, 0)

# version from sqlite3 in py3.6
sqlite_version = "3.34.1"
sqlite_version_info = (3, 34, 1)

Binary = bytes
Date = datetime.date
Time = datetime.time
Timestamp = datetime.datetime

Warning = SQLiteCloudWarning
Error = SQLiteCloudError
InterfaceError = SQLiteCloudInterfaceError
DatabaseError = SQLiteCloudDatabaseError
DataError = SQLiteCloudDataError
OperationalError = SQLiteCloudOperationalError
IntegrityError = SQLiteCloudIntegrityError
InternalError = SQLiteCloudInternalError
ProgrammingError = SQLiteCloudProgrammingError
NotSupportedError = SQLiteCloudNotSupportedError

# Map for types for SQLite
STRING = "TEXT"
BINARY = "BINARY"
NUMBER = "INTEGER"
DATETIME = "TIMESTAMP"
ROWID = "INTEGER PRIMARY KEY"

# SQLite supported types
SQLiteTypes = Union[int, float, str, bytes, None]

# Question mark style, e.g. ...WHERE name=?
# Module also supports Named style, e.g. ...WHERE name=:name
paramstyle = "qmark"

# Threads may share the module, but not connections
threadsafety = 1

# DB API level
apilevel = "2.0"

# These constants are meant to be used with the detect_types
# parameter of the connect() function
PARSE_DECLTYPES = 1
PARSE_COLNAMES = 2

# Adapters registry to convert Python types to SQLite types
adapters: Dict[Type[Any], Callable[[Any], SQLiteDataTypes]] = {}
# Converters registry to convert SQLite types to Python types
converters: Dict[str, Callable[[bytes], Any]] = {}


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
    detect_types: int = 0,
) -> "Connection":
    """
    Establishes a connection to the SQLite Cloud database.

    Args:
        connection_info (Union[str, SqliteCloudAccount]): The connection information.
            It can be either a connection string or a `SqliteCloudAccount` object.
        config (Optional[SQLiteCloudConfig]): The configuration options for the connection.
            Defaults to None.
        detect_types (int): Default (0), disabled. How data types not natively supported
            by SQLite are looked up to be converted to Python types, using the converters
            registered with register_converter().
            Accepts any combination (using |, bitwise or) of PARSE_DECLTYPES and PARSE_COLNAMES.
            Column names takes precedence over declared types if both flags are set.

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

    connection = Connection(
        driver.connect(config.account.hostname, config.account.port, config),
        detect_types=detect_types,
    )

    return connection


def register_adapter(
    pytype: Type, adapter_callable: Callable[[Any], SQLiteTypes]
) -> None:
    """
    Registers a callable to convert the type into one of the supported SQLite types.

    Args:
        type (Type): The type to convert.
        callable (Callable): The callable that converts the type into a supported
            SQLite supported type.
    """
    registry = _get_adapters_registry()
    registry[pytype] = adapter_callable


def register_converter(type_name: str, converter: Callable[[bytes], Any]) -> None:
    """
    Registers a callable to convert a bytestring from the database into a custom Python type.

    Args:
        type_name (str): The name of the type to convert.
            The match with the name of the type in the query is case-insensitive.
        converter (Callable): The callable that converts the bytestring into the custom Python type.
    """
    registry = _get_converters_registry()
    registry[type_name.lower()] = converter


def _get_adapters_registry() -> dict:
    return adapters


def _get_converters_registry() -> dict:
    return converters


class Connection:
    """
    Represents a DB-API 2.0 connection to the SQLite Cloud database.

    Args:
        sqlitecloud_connection (SQLiteCloudConnect): The SQLite Cloud connection object.

    Attributes:
        sqlitecloud_connection (SQLiteCloudConnect): The SQLite Cloud connection object.
    """

    def __init__(
        self, sqlitecloud_connection: SQLiteCloudConnect, detect_types: int = 0
    ) -> None:
        self._driver = Driver()
        self._autocommit = True
        self.sqlitecloud_connection = sqlitecloud_connection

        self.row_factory: Optional[Callable[["Cursor", Tuple], object]] = None
        self.text_factory: Union[Type[Union[str, bytes]], Callable[[bytes], Any]] = str

        self.detect_types = detect_types

        self.total_changes = 0

    @property
    def autocommit(self) -> bool:
        """Autocommit enabled is the only currently supported option in SQLite Cloud."""
        return self._autocommit

    @autocommit.setter
    def autocommit(self, value: bool) -> None:
        if not value:
            raise SQLiteCloudNotSupportedError("Disable Autocommit is not supported.")

    def execute(
        self,
        sql: str,
        parameters: Union[Tuple[any], Dict[Union[str, int], any]] = (),
    ) -> "Cursor":
        """
        Shortcut for cursor.execute().
        See the docstring of Cursor.execute() for more information.

        Args:
            sql (str): The SQL query to execute.
            parameters (Union[Tuple[any], Dict[Union[str, int], any]]):
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
        seq_of_parameters: Iterable[Union[Tuple[any], Dict[Union[str, int], any]]],
    ) -> "Cursor":
        """
        Shortcut for cursor.executemany().
        See the docstring of Cursor.executemany() for more information.

        Args:
            sql (str): The SQL statement to execute.
            seq_of_parameters (Iterable[Union[Tuple[any], Dict[Union[str, int], any]]]):
                The sequence of parameter sets to bind to the SQL statement.

        Returns:
            Cursor: The cursor object.
        """
        cursor = self.cursor()
        return cursor.executemany(sql, seq_of_parameters)

    def executescript(self, sql_script: str):
        raise SQLiteCloudNotSupportedError("executescript() is not supported.")

    def create_function(self, name, num_params, func):
        raise SQLiteCloudNotSupportedError("create_function() is not supported.")

    def create_aggregate(self, name, num_params, aggregate_class):
        raise SQLiteCloudNotSupportedError("create_aggregate() is not supported.")

    def create_collation(self, name, func):
        raise SQLiteCloudNotSupportedError("create_collation() is not supported.")

    def interrupt(self):
        raise SQLiteCloudNotSupportedError("interrupt() is not supported.")

    def set_authorizer(self, authorizer):
        raise SQLiteCloudNotSupportedError("set_authorizer() is not supported.")

    def set_progress_handler(self, handler, n):
        raise SQLiteCloudNotSupportedError("set_progress_handler() is not supported.")

    def set_trace_callback(self, trace_callback):
        raise SQLiteCloudNotSupportedError("set_trace_callback() is not supported.")

    def enable_load_extension(self, enable):
        raise SQLiteCloudNotSupportedError("enable_load_extension() is not supported.")

    def load_extension(path):
        raise SQLiteCloudNotSupportedError("load_extension() is not supported.")

    def iterdump(self):
        raise SQLiteCloudNotSupportedError("iterdump() is not supported.")

    def close(self):
        """
        Closes the database connection.
        All cursors created with this connection will become unusable after calling this method.

        Note:
            DB-API 2.0 interface does not manage the Sqlite Cloud PubSub feature.
            Therefore, only the main socket is closed.
        """
        self._driver.disconnect(self.sqlitecloud_connection, True)

    def is_connected(self) -> bool:
        """
        Check if the connection to SQLite Cloud database is still open.

        Returns:
            bool: True if the connection is open, False otherwise.
        """
        return self._driver.is_connected(self.sqlitecloud_connection)

    def commit(self):
        """
        Commit any pending transactions on database.
        """
        try:
            self._driver.execute("COMMIT;", self.sqlitecloud_connection)
        except SQLiteCloudOperationalError as e:
            if (
                e.errcode == 1
                and e.xerrcode == 1
                and "no transaction is active" in e.errmsg
            ):
                # compliance to sqlite3
                logging.debug(e)

    def rollback(self):
        """
        Causes the database to roll back to the start of any pending transaction.
        A transaction will also rool back if the database is closed or if an error occurs
        and the roll back conflict resolution algorithm is specified.

        See the documentation on the `ON CONFLICT <https://docs.sqlitecloud.io/docs/sqlite/lang_conflict>`
        clause for additional information about the ROLLBACK conflict resolution algorithm.
        """
        try:
            self._driver.execute("ROLLBACK;", self.sqlitecloud_connection)
        except SQLiteCloudOperationalError as e:
            if (
                e.errcode == 1
                and e.xerrcode == 1
                and "no transaction is active" in e.errmsg
            ):
                # compliance to sqlite3
                logging.debug(e)

    def cursor(self):
        """
        Creates a new cursor object.

        Returns:
            Cursor: The cursor object.
        """
        cursor = Cursor(self)
        cursor.row_factory = self.row_factory
        return cursor

    def _apply_adapter(self, value: Any) -> SQLiteTypes:
        """
        Applies the registered adapter to convert the Python type into a SQLite supported type.
        In the case there is no registered adapter, it calls the __conform__() method when the value object implements it.

        Args:
            value (Any): The Python type to convert.

        Returns:
            SQLiteTypes: The SQLite supported type or the given value when no adapter is found.
        """
        if type(value) in adapters:
            return adapters[type(value)](value)

        if hasattr(value, "__conform__"):
            # we don't support sqlite3.PrepareProtocol
            return value.__conform__(None)

        return value

    def __enter__(self):
        """
        Context manager to handle transactions.

        In sqlite3 module the control of the autocommit mode is governed by
        the `isolation_level` of the connection. To follow this behavior, the
        context manager does't start a new transaction implicitly. Instead,
        it handles the commit or rollback of transactions that are explicitly opened.
        """
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is None:
            self.commit()
        else:
            self.rollback()
            logging.error(
                f"Rolling back transaction - error '{exc_value}'",
                exc_info=True,
                extra={"traceback": traceback},
            )

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

    def __init__(self, connection: Connection) -> None:
        self._driver = Driver()
        self._connection = connection
        self._iter_row: int = 0
        self._resultset: SQLiteCloudResult = None
        self._result_operation: SQLiteCloudOperationResult = None

        self.row_factory: Optional[Callable[["Cursor", Tuple], object]] = None

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

        # Since py3.7:
        # bpo-39652: The column name found in sqlite3.Cursor.description is
        # now truncated on the first ‘[’ only if the PARSE_COLNAMES option is set.
        # https://github.com/python/cpython/issues/83833
        parse_colname = (
            self.connection.detect_types & PARSE_COLNAMES
        ) == PARSE_COLNAMES
        if sys.version_info < (3, 7):
            parse_colname = True

        description = ()
        for i in range(self._resultset.ncols):
            colname = self._resultset.colname[i]

            description += (
                (
                    self._parse_colname(colname)[0] if parse_colname else colname,
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
        The number of rows that the last .execute*() returned for DQL statements like SELECT or
        the number rows affected by DML statements like UPDATE, INSERT and DELETE.

        For the executemany() it returns the number of changes only for the last operation.
        """
        if self._is_result_rowset():
            return self._resultset.nrows
        if self._is_result_operation():
            return self._result_operation.changes
        return -1

    @property
    def lastrowid(self) -> Optional[int]:
        """
        Last rowid for DML operations (INSERT, UPDATE, DELETE).
        In case of `executemany()` it returns the last rowid of the last operation.
        """
        return (
            self._result_operation.rowid
            if self._result_operation and self._result_operation.rowid > 0
            else None
        )

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
        parameters: Union[Tuple[Any], Dict[Union[str, int], Any]] = (),
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
            parameters (Union[Tuple[any], Dict[Union[str, int], any]]):
                The parameters to be used in the query. It can be a tuple or a dictionary. (Default ())
            conn (SQLiteCloudConnect): The connection object to use for executing the query.

        Returns:
            Cursor: The cursor object.
        """
        self._ensure_connection()

        parameters = self._adapt_parameters(parameters)

        if isinstance(parameters, dict):
            parameters = self._named_to_question_mark_parameters(sql, parameters)

        result = self._driver.execute_statement(
            sql, parameters, self.connection.sqlitecloud_connection
        )

        self._reset()

        if isinstance(result, SQLiteCloudResult):
            self._resultset = result
        if isinstance(result, SQLiteCloudOperationResult):
            self._result_operation = result
            self._connection.total_changes = result.total_changes

        return self

    def executemany(
        self,
        sql: str,
        seq_of_parameters: Iterable[Union[Tuple[Any], Dict[Union[str, int], Any]]],
    ) -> "Cursor":
        """
        Executes a SQL statement multiple times, each with a different set of parameters.
        The entire statement is transmitted to the SQLite Cloud server in a single operation.
        This method is useful for executing the same query repeatedly with different values.

        Args:
            sql (str): The SQL statement to execute.
            seq_of_parameters (Iterable[Union[Tuple[any], Dict[Union[str, int], any]]]):
                The sequence of parameter sets to bind to the SQL statement.

        Returns:
            Cursor: The cursor object.
        """
        self._ensure_connection()

        commands = ""
        params = []
        for parameters in seq_of_parameters:
            if isinstance(parameters, dict):
                parameters = self._named_to_question_mark_parameters(sql, parameters)

            params += list(parameters)

            if not sql.endswith(";"):
                sql += ";"

            commands += sql

        self.execute(commands, params)

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
        raise SQLiteCloudNotSupportedError("setinputsizes() is not supported.")

    def setoutputsize(self, size, column=None) -> None:
        raise SQLiteCloudNotSupportedError("setoutputsize() is not supported.")

    def scroll(value, mode="relative"):
        raise SQLiteCloudNotSupportedError("scroll() is not supported.")

    def messages(self):
        raise SQLiteCloudNotSupportedError("messages() is not supported.")

    def _call_row_factory(self, row: Tuple) -> object:
        if self.row_factory is None:
            return row

        if self.row_factory is Row:
            return Row(row, [col[0] for col in self.description])

        return self.row_factory(self, row)

    def _is_result_rowset(self) -> bool:
        return (
            self._resultset
            and self._resultset.tag == SQLITECLOUD_RESULT_TYPE.RESULT_ROWSET
        )

    def _is_result_operation(self) -> bool:
        return self._result_operation is not None

    def _ensure_connection(self):
        """
        Ensure the cursor is usable or has been closed.

        Raises:
            SQLiteCloudException: If the cursor is closed.
        """
        if not self._connection or not self._connection.is_connected():
            raise SQLiteCloudProgrammingError("The cursor is closed.", code=1)

    def _adapt_parameters(self, parameters: Union[Dict, Tuple]) -> Union[Dict, Tuple]:
        if isinstance(parameters, dict):
            params = {}
            for i in parameters.keys():
                params[i] = self._connection._apply_adapter(parameters[i])
            return params

        return tuple(self._connection._apply_adapter(p) for p in parameters)

    def _convert_value(
        self, value: Any, colname: Optional[str], decltype: Optional[str]
    ) -> Any:
        if (
            colname
            and (self.connection.detect_types & PARSE_COLNAMES) == PARSE_COLNAMES
        ):
            try:
                return self._parse_colnames(value, colname)
            except MissingDecltypeException:
                pass

        if (
            decltype
            and (self.connection.detect_types & PARSE_DECLTYPES) == PARSE_DECLTYPES
        ):
            try:
                return self._parse_decltypes(value, decltype)
            except MissingDecltypeException:
                pass

        if decltype == SQLITECLOUD_VALUE_TYPE.TEXT.value or (
            decltype is None and isinstance(value, str)
        ):
            return self._apply_text_factory(value)

        return value

    def _parse_colname(self, colname: str) -> Tuple[str, str]:
        """
        Parse the column name to extract the column name and the
        declared type if present when it follows the syntax `colname [decltype]`.

        Args:
            colname (str): The column name with optional declared type.
                Eg: "mycol [mytype]"

        Returns:
            Tuple[str, str]: The column name and the declared type.
                Eg: ("mycol", "mytype")
        """
        # search for `[mytype]` in `mycol [mytype]`
        pattern = r"\[(.*?)\]"

        matches = re.findall(pattern, colname)
        if not matches or len(matches) == 0:
            return colname, None

        return colname.replace(f"[{matches[0]}]", "").strip(), matches[0]

    def _parse_colnames(self, value: Any, colname: str) -> Optional[Any]:
        """Convert the value using the explicit type in the column name."""
        _, decltype = self._parse_colname(colname)

        if decltype:
            return self._parse_decltypes(value, decltype)

        raise MissingDecltypeException(f"No decltype declared for: {decltype}")

    def _parse_decltypes(self, value: Any, decltype: str) -> Optional[Any]:
        """Convert the value by calling the registered converter for the given decltype."""
        decltype = decltype.lower()
        registry = _get_converters_registry()
        if decltype in registry:
            # sqlite3 always passes value as bytes
            value = (
                str(value).encode("utf-8") if not isinstance(value, bytes) else value
            )
            return registry[decltype](value)

        raise MissingDecltypeException(f"No decltype registered for: {decltype}")

    def _apply_text_factory(self, value: Any) -> Any:
        """Use Connection.text_factory to convert value with TEXT column or
        string value with undleclared column type."""

        if self._connection.text_factory is bytes:
            return value.encode("utf-8")
        if self._connection.text_factory is not str and callable(
            self._connection.text_factory
        ):
            return self._connection.text_factory(value.encode("utf-8"))

        return value

    def _named_to_question_mark_parameters(
        self, sql: str, params: Dict[str, Any]
    ) -> Tuple[Any]:
        """
        Convert named placeholders parameters from a dictionary to a list of
        parameters for question mark style.

        SCSP protocol does not support named placeholders yet.
        """
        # Python variable names: start with letter or underscore, followed by letters, digits, or underscores
        pattern = r":([a-zA-Z_][a-zA-Z0-9_]*)"
        matches = re.findall(pattern, sql)
        # filter out duplicates
        matches = list(dict.fromkeys(matches))

        params_list = ()
        for match in matches:
            if match in params:
                params_list += (params[match],)

        return params_list

    def _get_value(self, row: int, col: int) -> Optional[Any]:
        if not self._is_result_rowset():
            return None

        value = self._resultset.get_value(row, col)
        colname = self._resultset.get_name(col)
        decltype = self._resultset.get_decltype(col)

        return self._convert_value(value, colname, decltype)

    def _reset(self) -> None:
        self._resultset = None
        self._result_operation = None

        self._iter_row = 0

    def __iter__(self) -> "Cursor":
        return self

    def __next__(self) -> Optional[Tuple[Any]]:
        if (
            self._resultset
            and not self._resultset.is_result
            and self._resultset.data
            and self._iter_row < self._resultset.nrows
        ):
            out: Tuple[Any] = ()

            for col in range(self._resultset.ncols):
                out += (self._get_value(self._iter_row, col),)
            self._iter_row += 1

            return self._call_row_factory(out)

        raise StopIteration


class Row:
    def __init__(self, data: Tuple[Any], column_names: List[str]):
        """
        Initialize the Row object with data and column names.

        Args:
            data (Tuple[Any]): A tuple containing the row data.
            column_names (List[str]): A list of column names corresponding to the data.
        """
        self._data = data
        self._column_names = column_names
        self._column_map = {name.lower(): idx for idx, name in enumerate(column_names)}

    def keys(self) -> List[str]:
        """Return the column names."""
        return self._column_names

    def __getitem__(self, key):
        """Support indexing by both column name and index."""
        if isinstance(key, int):
            return self._data[key]
        elif isinstance(key, str):
            return self._data[self._column_map[key.lower()]]
        else:
            raise TypeError("Invalid key type. Must be int or str.")

    def __len__(self) -> int:
        return len(self._data)

    def __iter__(self) -> Iterator[Any]:
        return iter(self._data)

    def __repr__(self) -> str:
        return "\n".join(
            f"{name}: {self._data[idx]}" for idx, name in enumerate(self._column_names)
        )

    def __hash__(self) -> int:
        return hash((self._data, tuple(self._column_map)))

    def __eq__(self, other) -> bool:
        """Check if both have the same data and column names."""
        if not isinstance(other, Row):
            return NotImplemented

        return self._data == other._data and self._column_map == other._column_map

    def __ne__(self, other):
        if not isinstance(other, Row):
            return NotImplemented
        return not self.__eq__(other)


class MissingDecltypeException(Exception):
    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message


def DateFromTicks(ticks):
    return Date(*time.localtime(ticks)[:3])


def TimeFromTicks(ticks):
    return Time(*time.localtime(ticks)[3:6])


def TimestampFromTicks(ticks):
    return Timestamp(*time.localtime(ticks)[:6])


def register_adapters_and_converters():
    """
    sqlite3 default adapters and converters.

    This code comes from the Python standard library's sqlite3 module.
    The Python standard library is licensed under the Python Software Foundation License.
    Source: https://github.com/python/cpython/blob/3.6/Lib/sqlite3/dbapi2.py
    """

    def adapt_date(val):
        return val.isoformat()

    def adapt_datetime(val):
        return val.isoformat(" ")

    def convert_date(val):
        return datetime.date(*map(int, val.split(b"-")))

    def convert_timestamp(val):
        datepart, timepart = val.split(b" ")
        year, month, day = map(int, datepart.split(b"-"))
        timepart_full = timepart.split(b".")
        hours, minutes, seconds = map(int, timepart_full[0].split(b":"))
        if len(timepart_full) == 2:
            microseconds = int("{:0<6.6}".format(timepart_full[1].decode()))
        else:
            microseconds = 0

        val = datetime.datetime(year, month, day, hours, minutes, seconds, microseconds)
        return val

    register_adapter(datetime.date, adapt_date)
    register_adapter(datetime.datetime, adapt_datetime)
    register_converter("date", convert_date)
    register_converter("timestamp", convert_timestamp)


register_adapters_and_converters()
collections.abc.Sequence.register(Row)
