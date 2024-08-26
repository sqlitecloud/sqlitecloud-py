# DBAPI 2.0 Exceptions Hierachy
#
# Exception
# |__Warning
# |__Error
#    |__InterfaceError
#    |__DatabaseError
#       |__DataError
#       |__OperationalError
#       |__IntegrityError
#       |__InternalError
#       |__ProgrammingError
#       |__NotSupportedError


class SQLiteCloudWarning(Exception):
    def __init__(self, message: str, code: int = -1, xerrcode: int = 0) -> None:
        super().__init__(message)
        self.errmsg = f"Warning: {message}"
        self.errcode = code
        self.xerrcode = xerrcode


class SQLiteCloudError(Exception):
    def __init__(self, message: str, code: int = -1, xerrcode: int = 0) -> None:
        super().__init__(message)
        self.errmsg = message
        self.errcode = code
        self.xerrcode = xerrcode


class SQLiteCloudInterfaceError(SQLiteCloudError):
    def __init__(self, message: str, code: int = -1, xerrcode: int = 0) -> None:
        super().__init__(message, code, xerrcode)


class SQLiteCloudDatabaseError(SQLiteCloudError):
    def __init__(self, message: str, code: int = -1, xerrcode: int = 0) -> None:
        super().__init__(message, code, xerrcode)


class SQLiteCloudDataError(SQLiteCloudDatabaseError):
    def __init__(self, message: str, code: int = -1, xerrcode: int = 0) -> None:
        super().__init__(message, code, xerrcode)


class SQLiteCloudOperationalError(SQLiteCloudDatabaseError):
    def __init__(self, message: str, code: int = -1, xerrcode: int = 0) -> None:
        super().__init__(message, code, xerrcode)


class SQLiteCloudIntegrityError(SQLiteCloudDatabaseError):
    def __init__(self, message: str, code: int = -1, xerrcode: int = 0) -> None:
        super().__init__(message, code, xerrcode)


class SQLiteCloudInternalError(SQLiteCloudDatabaseError):
    def __init__(self, message: str, code: int = -1, xerrcode: int = 0) -> None:
        super().__init__(message, code, xerrcode)


class SQLiteCloudProgrammingError(SQLiteCloudDatabaseError):
    def __init__(self, message: str, code: int = -1, xerrcode: int = 0) -> None:
        super().__init__(message, code, xerrcode)


class SQLiteCloudNotSupportedError(SQLiteCloudDatabaseError):
    def __init__(self, message: str, code: int = -1, xerrcode: int = 0) -> None:
        super().__init__(
            f"The feature is not implemented in SQLite Cloud\n{message}", code, xerrcode
        )


class SQLiteCloudException(SQLiteCloudError):
    def __init__(self, message: str, code: int = -1, xerrcode: int = 0) -> None:
        super().__init__(message, code, xerrcode)


def get_sqlitecloud_error_with_extended_code(
    message: str, code: int, xerrcode: int
) -> None:
    """Mapping of sqlite error codes: https://www.sqlite.org/rescode.html"""

    # define base error codes and their corresponding exceptions
    base_error_mapping = {
        1: SQLiteCloudOperationalError,  # SQLITE_ERROR
        2: SQLiteCloudInternalError,  # SQLITE_INTERNAL
        3: SQLiteCloudOperationalError,  # SQLITE_PERM
        4: SQLiteCloudOperationalError,  # SQLITE_ABORT
        5: SQLiteCloudOperationalError,  # SQLITE_BUSY
        6: SQLiteCloudOperationalError,  # SQLITE_LOCKED
        7: SQLiteCloudDatabaseError,  # SQLITE_NOMEM
        8: SQLiteCloudOperationalError,  # SQLITE_READONLY
        9: SQLiteCloudOperationalError,  # SQLITE_INTERRUPT
        10: SQLiteCloudOperationalError,  # SQLITE_IOERR
        11: SQLiteCloudDatabaseError,  # SQLITE_CORRUPT
        12: SQLiteCloudOperationalError,  # SQLITE_NOTFOUND
        13: SQLiteCloudOperationalError,  # SQLITE_FULL
        14: SQLiteCloudOperationalError,  # SQLITE_CANTOPEN
        15: SQLiteCloudOperationalError,  # SQLITE_PROTOCOL
        16: SQLiteCloudDatabaseError,  # SQLITE_EMPTY
        17: SQLiteCloudOperationalError,  # SQLITE_SCHEMA
        18: SQLiteCloudDataError,  # SQLITE_TOOBIG
        19: SQLiteCloudIntegrityError,  # SQLITE_CONSTRAINT
        20: SQLiteCloudProgrammingError,  # SQLITE_MISMATCH
        21: SQLiteCloudProgrammingError,  # SQLITE_MISUSE
        22: SQLiteCloudOperationalError,  # SQLITE_NOLFS
        23: SQLiteCloudOperationalError,  # SQLITE_AUTH
        24: SQLiteCloudDatabaseError,  # SQLITE_FORMAT
        25: SQLiteCloudDataError,  # SQLITE_RANGE
        26: SQLiteCloudDatabaseError,  # SQLITE_NOTADB
        27: SQLiteCloudWarning,  # SQLITE_NOTICE
        28: SQLiteCloudWarning,  # SQLITE_WARNING
        100: SQLiteCloudWarning,  # SQLITE_ROW (not an error)
        101: SQLiteCloudWarning,  # SQLITE_DONE (not an error)
    }

    # define extended error codes and their corresponding exceptions
    extended_error_mapping = {
        257: SQLiteCloudOperationalError,  # SQLITE_ERROR_MISSING_COLLSEQ
        279: SQLiteCloudOperationalError,  # SQLITE_AUTH_USER
        266: SQLiteCloudOperationalError,  # SQLITE_IOERR_READ
        522: SQLiteCloudOperationalError,  # SQLITE_IOERR_SHORT_READ
        778: SQLiteCloudOperationalError,  # SQLITE_IOERR_WRITE
        1034: SQLiteCloudOperationalError,  # SQLITE_IOERR_FSYNC
        1290: SQLiteCloudOperationalError,  # SQLITE_IOERR_DIR_FSYNC
        1546: SQLiteCloudOperationalError,  # SQLITE_IOERR_TRUNCATE
        1802: SQLiteCloudOperationalError,  # SQLITE_IOERR_FSTAT
        2058: SQLiteCloudOperationalError,  # SQLITE_IOERR_UNLOCK
        2314: SQLiteCloudOperationalError,  # SQLITE_IOERR_RDLOCK
        2570: SQLiteCloudOperationalError,  # SQLITE_IOERR_DELETE
        2826: SQLiteCloudOperationalError,  # SQLITE_IOERR_BLOCKED
        3082: SQLiteCloudOperationalError,  # SQLITE_IOERR_NOMEM
        3338: SQLiteCloudOperationalError,  # SQLITE_IOERR_ACCESS
        3594: SQLiteCloudOperationalError,  # SQLITE_IOERR_CHECKRESERVEDLOCK
        3850: SQLiteCloudOperationalError,  # SQLITE_IOERR_LOCK
        4106: SQLiteCloudOperationalError,  # SQLITE_IOERR_CLOSE
        4362: SQLiteCloudOperationalError,  # SQLITE_IOERR_DIR_CLOSE
        4618: SQLiteCloudOperationalError,  # SQLITE_IOERR_SHMOPEN
        4874: SQLiteCloudOperationalError,  # SQLITE_IOERR_SHMSIZE
        5130: SQLiteCloudOperationalError,  # SQLITE_IOERR_SHMLOCK
        5386: SQLiteCloudOperationalError,  # SQLITE_IOERR_SHMMAP
        5642: SQLiteCloudOperationalError,  # SQLITE_IOERR_SEEK
        5898: SQLiteCloudOperationalError,  # SQLITE_IOERR_DELETE_NOENT
        6154: SQLiteCloudOperationalError,  # SQLITE_IOERR_MMAP
        6410: SQLiteCloudOperationalError,  # SQLITE_IOERR_GETTEMPPATH
        6666: SQLiteCloudOperationalError,  # SQLITE_IOERR_CONVPATH
        6922: SQLiteCloudOperationalError,  # SQLITE_IOERR_VNODE
        7178: SQLiteCloudOperationalError,  # SQLITE_IOERR_AUTH
        262: SQLiteCloudOperationalError,  # SQLITE_LOCKED_SHAREDCACHE
        261: SQLiteCloudOperationalError,  # SQLITE_BUSY_RECOVERY
        517: SQLiteCloudOperationalError,  # SQLITE_BUSY_SNAPSHOT
        270: SQLiteCloudOperationalError,  # SQLITE_CANTOPEN_NOTEMPDIR
        526: SQLiteCloudOperationalError,  # SQLITE_CANTOPEN_ISDIR
        782: SQLiteCloudOperationalError,  # SQLITE_CANTOPEN_FULLPATH
        1038: SQLiteCloudOperationalError,  # SQLITE_CANTOPEN_CONVPATH
        264: SQLiteCloudOperationalError,  # SQLITE_READONLY_RECOVERY
        520: SQLiteCloudOperationalError,  # SQLITE_READONLY_CANTLOCK
        776: SQLiteCloudOperationalError,  # SQLITE_READONLY_ROLLBACK
        1032: SQLiteCloudOperationalError,  # SQLITE_READONLY_DBMOVED
        1544: SQLiteCloudOperationalError,  # SQLITE_READONLY_DIRECTORY
        516: SQLiteCloudOperationalError,  # SQLITE_ABORT_ROLLBACK
        275: SQLiteCloudIntegrityError,  # SQLITE_CONSTRAINT_CHECK
        531: SQLiteCloudIntegrityError,  # SQLITE_CONSTRAINT_COMMITHOOK
        787: SQLiteCloudIntegrityError,  # SQLITE_CONSTRAINT_FOREIGNKEY
        1043: SQLiteCloudIntegrityError,  # SQLITE_CONSTRAINT_FUNCTION
        1299: SQLiteCloudIntegrityError,  # SQLITE_CONSTRAINT_NOTNULL
        1555: SQLiteCloudIntegrityError,  # SQLITE_CONSTRAINT_PRIMARYKEY
        1811: SQLiteCloudIntegrityError,  # SQLITE_CONSTRAINT_TRIGGER
        2067: SQLiteCloudIntegrityError,  # SQLITE_CONSTRAINT_UNIQUE
        2323: SQLiteCloudIntegrityError,  # SQLITE_CONSTRAINT_VTAB
        2579: SQLiteCloudIntegrityError,  # SQLITE_CONSTRAINT_ROWID
        779: SQLiteCloudDatabaseError,  # SQLITE_CORRUPT_INDEX
        523: SQLiteCloudProgrammingError,  # SQLITE_CORRUPT_SEQUENCE
        3091: SQLiteCloudDataError,  # SQLITE_CONSTRAINT_DATATYPE
        2835: SQLiteCloudDataError,  # SQLITE_CONSTRAINT_PINNED
        539: SQLiteCloudWarning,  # SQLITE_NOTICE_RECOVER_WAL
        283: SQLiteCloudWarning,  # SQLITE_NOTICE_RECOVER_ROLLBACK
        284: SQLiteCloudWarning,  # SQLITE_WARNING_AUTOINDEX
    }

    error_mapping = {**base_error_mapping, **extended_error_mapping}

    # retrieve the corresponding exception based on the error code
    exception = error_mapping.get(xerrcode, error_mapping.get(code, SQLiteCloudError))

    return exception
