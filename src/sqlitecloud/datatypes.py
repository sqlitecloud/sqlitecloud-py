from asyncio import AbstractEventLoop
from enum import Enum
from typing import Any, Callable, Dict, Optional, Union
from urllib import parse

from sqlitecloud.exceptions import SQLiteCloudException

from .resultset import SQLiteCloudResultSet

# SQLite supported data types
SQLiteDataTypes = Union[str, int, float, bytes, None]


# Basic types supported by SQLite Cloud APIs
SQLiteCloudDataTypes = Union[str, int, bool, Dict[Union[str, int], Any], bytes, None]


class SQLITECLOUD_DEFAULT(Enum):
    PORT = 8860
    TIMEOUT = 30
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
        token: Optional[str] = "",
    ) -> None:
        # User name is required unless connectionstring is provided
        self.username = username
        # Password is required unless connection string is provided
        self.password = password
        # Password is hashed
        self.password_hashed = False
        # API key
        self.apikey = apikey
        # Access Token
        self.token = token
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

        self.pubsub_socket: any = None
        self.pubsub_callback: Callable[
            [SQLiteCloudConnect, Optional[SQLiteCloudResultSet], Optional[any]],
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

        # Compression enabled by default
        self.compression = True
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

                # alias
                if opt == "nonlinearizable":
                    opt = "non_linearizable"

                if hasattr(self, opt):
                    setattr(self, opt, value)
                elif hasattr(self.account, opt):
                    setattr(self.account, opt, value)

            if params.username and params.password:
                self.account.username = parse.unquote(params.username)
                self.account.password = parse.unquote(params.password)

            # either you use an apikey, token or username and password
            if (
                bool(self.account.apikey)
                + bool(self.account.token)
                + bool(self.account.username or self.account.password)
                > 1
            ):
                raise SQLiteCloudException(
                    "Choose between apikey, token or username/password"
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
            raise SQLiteCloudException("Invalid connection string") from e


class SQLiteCloudNumber:
    """
    Represents the parsed number or the error code.
    """

    def __init__(self) -> None:
        self.value: Optional[int] = None
        self.cstart: int = 0
        self.extcode: int = None
        self.offcode: int = None


class SQLiteCloudValue:
    """
    Represents the parse value.
    """

    def __init__(self) -> None:
        self.value: Optional[SQLiteCloudDataTypes] = None
        self.len: int = 0
        self.cellsize: int = 0
