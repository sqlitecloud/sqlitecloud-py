import sqlalchemy.types as sqltypes
from sqlalchemy import util
from sqlalchemy.dialects.sqlite.base import DATE, DATETIME, SQLiteDialect
from sqlalchemy.engine.url import URL
from sqlalchemy.exc import ArgumentError
from sqlalchemy.pool import NullPool


class _SQLite_pysqliteTimeStamp(DATETIME):
    def bind_processor(self, dialect):
        if dialect.native_datetime:
            return None
        else:
            return DATETIME.bind_processor(self, dialect)

    def result_processor(self, dialect, coltype):
        if dialect.native_datetime:
            return None
        else:
            return DATETIME.result_processor(self, dialect, coltype)


class _SQLite_pysqliteDate(DATE):
    def bind_processor(self, dialect):
        if dialect.native_datetime:
            return None
        else:
            return DATE.bind_processor(self, dialect)

    def result_processor(self, dialect, coltype):
        if dialect.native_datetime:
            return None
        else:
            return DATE.result_processor(self, dialect, coltype)


class SQLiteCloudDialect(SQLiteDialect):
    name = "sqlitecloud"
    driver = "sqlitecloud"

    default_paramstyle = "qmark"
    supports_statement_cache = False

    colspecs = util.update_copy(
        SQLiteDialect.colspecs,
        {
            sqltypes.Date: _SQLite_pysqliteDate,
            sqltypes.TIMESTAMP: _SQLite_pysqliteTimeStamp,
        },
    )

    @classmethod
    def dbapi(cls):
        from sqlitecloud import dbapi2

        return dbapi2

    @classmethod
    def get_pool_class(cls, url):
        return NullPool

    def _get_server_version_info(self, connection):
        return self.dbapi.sqlite_version_info

    def set_isolation_level(self, connection, level):
        if level != "AUTOCOMMIT":
            raise ArgumentError(
                "SQLite Cloud supports only AUTOCOMMIT isolation level."
            )

        if hasattr(connection, "dbapi_connection"):
            dbapi_connection = connection.dbapi_connection
        else:
            dbapi_connection = connection

        if level == "AUTOCOMMIT":
            dbapi_connection.isolation_level = None
        else:
            dbapi_connection.isolation_level = ""
            return super(SQLiteCloudDialect, self).set_isolation_level(
                connection, level
            )

    def on_connect(self):
        connect = super(SQLiteCloudDialect, self).on_connect()

        fns = []

        if self.isolation_level is not None:

            def iso_level(conn):
                self.set_isolation_level(conn, self.isolation_level)

            fns.append(iso_level)

        def connect(conn):  # noqa: F811
            for fn in fns:
                fn(conn)

        return connect

    def create_connect_args(self, url: URL):
        if not url.host:
            raise ArgumentError(
                "SQLite Cloud URL is required.\n"
                "Register on https://sqlitecloud.io/ to get your free SQLite Cloud account.\n"
                "Valid SQLite Cloud URL are:\n"
                " sqlitecloud:///myuser:mypass@myserver.sqlite.cloud/mydb.sqlite?non_linearizable=true\n"
                " sqlitecloud:///myserver.sqlite.cloud/?apikey=mykey1234"
            )

        # TODO: this should be the list of SQLite Cloud Config params
        pysqlite_args = [
            # ("timeout", float),
            # ("isolation_level", str),
            ("detect_types", int),
        ]
        opts = url.query
        pysqlite_opts = {}
        for key, type_ in pysqlite_args:
            util.coerce_kw_type(opts, key, type_, dest=pysqlite_opts)

        # sqlitecloud//...
        url = url.set(drivername="sqlitecloud")

        return ([url.render_as_string(hide_password=False)], pysqlite_opts)

    def is_disconnect(self, e, connection, cursor):
        return isinstance(
            e, self.dbapi.ProgrammingError
        ) and "Cannot operate on a closed database." in str(e)


dialect = SQLiteCloudDialect
