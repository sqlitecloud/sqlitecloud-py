import types
from asyncio import AbstractEventLoop
from enum import Enum
from typing import Any, Callable, Dict, Optional, Union
from urllib import parse

# Basic types supported by SQLite Cloud APIs
SQLiteCloudDataTypes = Union[str, int, bool, Dict[Union[str, int], Any], bytes, None]


class SQLITECLOUD_DEFAULT(Enum):
    PORT = 8860
    TIMEOUT = 12
    UPLOAD_SIZE = 512 * 1024


class SQLITECLOUD_CMD(Enum):
    STRING = "+"
    ZEROSTRING = "!"
    ERROR = "-"
    INT = ":"
    FLOAT = ","
    ROWSET = "*"
    ROWSET_CHUNK = "/"
    JSON = "#"
    RAWJSON = "{"
    NULL = "_"
    BLOB = "$"
    COMPRESSED = "%"
    PUBSUB = "|"
    COMMAND = "^"
    RECONNECT = "@"
    ARRAY = "="


class SQLITECLOUD_ROWSET(Enum):
    CHUNKS_END = b"/6 0 0 0 "


class SQLITECLOUD_VALUE_TYPE(Enum):
    INTEGER = "INTEGER"
    FLOAT = "REAL"
    TEXT = "TEXT"
    BLOB = "BLOB"
    NULL = "NULL"


class SQLITECLOUD_INTERNAL_ERRCODE(Enum):
    """
    Clients error codes.
    """

    NONE = 0
    NETWORK = 100005


class SQLITECLOUD_ERRCODE(Enum):
    """
    Error codes from Sqlite Cloud.
    """

    MEM = 10000
    NOTFOUND = 10001
    COMMAND = 10002
    INTERNAL = 10003
    AUTH = 10004
    GENERIC = 10005
    RAFT = 10006


class SQLITECLOUD_RESULT_TYPE(Enum):
    RESULT_OK = 0
    RESULT_ERROR = 1
    RESULT_STRING = 2
    RESULT_INTEGER = 3
    RESULT_FLOAT = 4
    RESULT_ROWSET = 5
    RESULT_ARRAY = 6
    RESULT_NONE = 7
    RESULT_JSON = 8
    RESULT_BLOB = 9


class SQLITECLOUD_PUBSUB_SUBJECT(Enum):
    """
    Subjects that can be subscribed to by PubSub.
    """

    TABLE = "TABLE"
    CHANNEL = "CHANNEL"


class SQLiteCloudRowsetSignature:
    """
    Represents the parsed signature for a rowset.
    """

    def __init__(self) -> None:
        self.start: int = -1
        self.len: int = 0
        self.idx: int = 0
        self.version: int = 0
        self.nrows: int = 0
        self.ncols: int = 0


class SQLiteCloudAccount:
    def __init__(
        self,
        username: Optional[str] = "",
        password: Optional[str] = "",
        hostname: str = "",
        dbname: Optional[str] = "",
        port: int = SQLITECLOUD_DEFAULT.PORT.value,
        apikey: Optional[str] = "",
    ) -> None:
        # User name is required unless connectionstring is provided
        self.username = username
        # Password is required unless connection string is provided
        self.password = password
        # Password is hashed
        self.password_hashed = False
        # API key instead of username and password
        self.apikey = apikey
        # Name of database to open
        self.dbname = dbname
        # Like mynode.sqlitecloud.io
        self.hostname = hostname
        self.port = port


class SQLiteCloudConnect:
    """
    Represents the connection information.
    """

    def __init__(self):
        self.socket: any = None
        self.config: SQLiteCloudConfig
        self.isblob: bool = False

        self.pubsub_socket: any = None
        self.pubsub_callback: Callable[
            [SQLiteCloudConnect, Optional[types.SqliteCloudResultSet], Optional[any]],
            None,
        ] = None
        self.pubsub_data: any = None
        self.pubsub_thread: AbstractEventLoop = None


class SQLiteCloudConfig:
    def __init__(self, connection_str: Optional[str] = None) -> None:
        self.account: SQLiteCloudAccount = None

        # Optional query timeout passed directly to TLS socket
        self.timeout = 0
        # Socket connection timeout
        self.connect_timeout = SQLITECLOUD_DEFAULT.TIMEOUT.value

        # Enable compression
        self.compression = False
        # Tell the server to zero-terminate strings
        self.zerotext = False
        # Database will be created in memory
        self.memory = False
        # Create the database if it doesn't exist?
        self.create = False
        # Request for immediate responses from the server node without waiting for linerizability guarantees
        self.non_linearizable = False
        # Connect using plain TCP port, without TLS encryption, NOT RECOMMENDED
        self.insecure = False
        # Accept invalid TLS certificates
        self.no_verify_certificate = False

        # Filepath of certificates
        self.root_certificate: str = None
        self.certificate: str = None
        self.certificate_key: str = None

        # Server should send BLOB columns
        self.noblob = False
        # Do not send columns with more than max_data bytes
        self.maxdata = 0
        # Server should chunk responses with more than maxRows
        self.maxrows = 0
        # Server should limit total number of rows in a set to maxRowset
        self.maxrowset = 0

        if connection_str is not None:
            self._parse_connection_string(connection_str)

    def _parse_connection_string(self, connection_string) -> None:
        # URL STRING FORMAT
        # sqlitecloud://user:pass@host.com:port/dbname?timeout=10&key2=value2&key3=value3
        # or sqlitecloud://host.sqlite.cloud:8860/dbname?apikey=zIiAARzKm9XBVllbAzkB1wqrgijJ3Gx0X5z1A4m4xBA

        self.account = SQLiteCloudAccount()

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

                # alias
                if opt == "nonlinearizable":
                    opt = "non_linearizable"

                if hasattr(self, opt):
                    setattr(self, opt, value)
                elif hasattr(self.account, opt):
                    setattr(self.account, opt, value)

            # apikey or username/password is accepted
            if not self.account.apikey:
                self.account.username = (
                    parse.unquote(params.username) if params.username else ""
                )
                self.account.password = (
                    parse.unquote(params.password) if params.password else ""
                )

            path = params.path
            database = path.strip("/")
            if database:
                self.account.dbname = database

            self.account.hostname = params.hostname
            self.account.port = (
                int(params.port) if params.port else SQLITECLOUD_DEFAULT.PORT.value
            )
        except Exception as e:
            raise SQLiteCloudException(
                f"Invalid connection string {connection_string}"
            ) from e


class SQLiteCloudException(Exception):
    def __init__(self, message: str, code: int = -1, xerrcode: int = 0) -> None:
        self.errmsg = str(message)
        self.errcode = code
        self.xerrcode = xerrcode


class SQLiteCloudNumber:
    """
    Represents the parsed number or the error code.
    """

    def __init__(self) -> None:
        self.value: Optional[int] = None
        self.cstart: int = 0
        self.extcode: int = None


class SQLiteCloudValue:
    """
    Represents the parse value.
    """

    def __init__(self) -> None:
        self.value: Optional[str] = None
        self.len: int = 0
        self.cellsize: int = 0
