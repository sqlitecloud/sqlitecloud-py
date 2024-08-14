# To replicate the public interface of sqlite3, we need to import
# the classes and functions from the dbapi2 module.
# eg:  sqlite3.connect() -> sqlitecloud.connect()
#
from .dbapi2 import (
    PARSE_COLNAMES,
    PARSE_DECLTYPES,
    Connection,
    Cursor,
    adapters,
    connect,
    converters,
    register_adapter,
    register_converter,
)

__all__ = [
    "VERSION",
    "Connection",
    "Cursor",
    "connect",
    "register_adapter",
    "register_converter",
    "PARSE_DECLTYPES",
    "PARSE_COLNAMES",
    "adapters",
    "converters",
]

VERSION = "0.0.79"
