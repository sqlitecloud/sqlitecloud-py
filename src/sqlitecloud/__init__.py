# To replicate the public interface of sqlite3, we need to import
# the classes and functions from the dbapi2 module.
# eg:  sqlite3.connect() -> sqlitecloud.connect()
#
from .dbapi2 import Connection, Cursor, connect, register_adapter

__all__ = ["VERSION", "Connection", "Cursor", "connect", "register_adapter"]

VERSION = "0.0.79"
