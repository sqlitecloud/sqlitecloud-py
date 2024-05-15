from enum import Enum
from typing import Optional
from enum import Enum


class SQCLOUD_VALUE_TYPE(Enum):
    VALUE_INTEGER = 1
    VALUE_FLOAT = 2
    VALUE_TEXT = 3
    VALUE_BLOB = 4
    VALUE_NULL = 5


class SQCLOUD_RESULT_TYPE(Enum):
    RESULT_OK = 0
    RESULT_FLOAT = 2
    RESULT_STRING = 2
    RESULT_INTEGER = 3
    RESULT_ERROR = 4
    RESULT_ROWSET = 5
    RESULT_ARRAY = 6
    RESULT_NULL = 7
    RESULT_JSON = 8
    RESULT_BLOB = 9


class SQCLOUD_CMD(Enum):
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


class SQCLOUD_ROWSET(Enum):
    CHUNKS_END = b"/6 0 0 0 "


class SQCLOUD_INTERNAL_ERRCODE(Enum):
    INTERNAL_ERRCODE_NONE = 0
    # INTERNAL_ERRCODE_GENERIC = 100000
    # INTERNAL_ERRCODE_PUBSUB = 100001
    # INTERNAL_ERRCODE_TLS = 100002
    # INTERNAL_ERRCODE_URL = 100003
    # INTERNAL_ERRCODE_MEMORY = 100004
    INTERNAL_ERRCODE_NETWORK = 100005
    # INTERNAL_ERRCODE_FORMAT = 100006
    # INTERNAL_ERRCODE_INDEX = 100007
    # INTERNAL_ERRCODE_SOCKCLOSED = 100008


class SQCloudRowsetSignature:
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


class SqliteCloudAccount:
    def __init__(self):
        # User name is required unless connectionstring is provided
        self.username = ""
        # Password is required unless connection string is provided
        self.password = ""
        # Password is hashed
        self.password_hashed = False
        # API key instead of username and password
        self.apikey = ""
        # Name of database to open
        self.database = ""
        # Like mynode.sqlitecloud.io
        self.hostname = ""
        self.port = 8860


class SQCloudConnect:
    def __init__(self):
        self.hostname: str = ""
        self.port: int = ""

        self.socket: any = None

        self.config: SQCloudConfig

        self.isblob: bool = False
        self.config_to_free: bool  # todo: is this needed?

        # pub/sub
        # todo: check uuid type
        self.uuid: str

        # todo:
        # pubsubfd: int

        # callback: SQCloudPubSubCB


class SQCloudConfig:
    def __init__(self) -> None:
        self.account: SqliteCloudAccount = None

        # Optional query timeout passed directly to TLS socket
        self.timeout = 0
        # Socket connection timeout
        self.connect_timeout = 20

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


class SQCloudException(Exception):
    def __init__(
        self, message: str, code: Optional[int] = -1, xerrcode: Optional[int] = 0
    ) -> None:
        self.errmsg = str(message)
        self.errcode = code
        self.xerrcode = xerrcode


class SQCloudNumber:
    """
    Represents the parsed number or the error code.
    """

    def __init__(self) -> None:
        self.value: Optional[int] = None
        self.cstart: int = 0
        self.extcode: int = None


class SQCloudValue:
    """
    Represents the parse value.
    """

    def __init__(self) -> None:
        self.value: Optional[str] = None
        self.len: int = 0
        self.cellsize: int = 0
