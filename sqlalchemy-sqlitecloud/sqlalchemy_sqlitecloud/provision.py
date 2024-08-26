import os

from dotenv import load_dotenv
from sqlalchemy.engine import make_url
from sqlalchemy.pool import Pool
from sqlalchemy.testing.provision import (
    generate_driver_url,
    post_configure_engine,
    stop_test_class_outside_fixtures,
    temp_table_keyword_args,
)

_drivernames = {
    "sqlitecloud",
}


@generate_driver_url.for_db("sqlitecloud")
def generate_driver_url(url, driver, query_str):
    # no database specified here, it's created and used later
    # eg: sqlitecloud://mynode.sqlite.cloud/?apikey=key123

    load_dotenv("../../.env")

    connection_string = os.getenv("SQLITE_CONNECTION_STRING")
    apikey = os.getenv("SQLITE_API_KEY")

    connection_string += f"?apikey={apikey}"

    return make_url(connection_string)


@post_configure_engine.for_db("sqlitecloud")
def _sqlite_post_configure_engine(url, engine, follower_ident):
    from sqlalchemy import event

    main_database = "sqlalchemy_sqlitecloud.db"
    attached_database = "sqlalchemy_sqlitecloud_test_schema.db"

    _create_dbs(url, main_database)
    _create_dbs(url, attached_database)

    @event.listens_for(Pool, "connect")
    def connect(dbapi_connection, connection_record):
        # attach the test schema for all new connections,
        # not just when the engine is first created and connected.
        # Next connections lost the attached schema.
        dbapi_connection.execute(f"USE DATABASE {main_database}")
        dbapi_connection.execute(
            f'ATTACH DATABASE "{attached_database}" AS test_schema'
        )


def _create_dbs(url, database):
    import sqlitecloud

    with sqlitecloud.connect(str(url)) as conn:
        conn.execute(
            f"REMOVE DATABASE {database} IF EXISTS; "
            f"CREATE DATABASE {database} IF NOT EXISTS"
        )


@stop_test_class_outside_fixtures.for_db("sqlitecloud")
def stop_test_class_outside_fixtures(config, db, cls):
    db.dispose()


@temp_table_keyword_args.for_db("sqlitecloud")
def _sqlite_temp_table_keyword_args(cfg, eng):
    return {"prefixes": ["TEMPORARY"]}
