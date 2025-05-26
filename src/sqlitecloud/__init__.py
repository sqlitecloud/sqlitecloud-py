# To replicate the public interface of sqlite3,
# we need to import everything from the dbapi2 module.
# eg:  sqlite3.connect() -> sqlitecloud.connect()
#
from .dbapi2 import *  # noqa

VERSION = "0.0.84"
