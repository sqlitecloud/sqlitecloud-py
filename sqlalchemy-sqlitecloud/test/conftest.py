import os

import pytest
from dotenv import find_dotenv, load_dotenv

pytest.register_assert_rewrite("sqlalchemy.testing.assertions")

from sqlalchemy.testing.plugin.pytestplugin import *  # noqa
from sqlalchemy.testing.plugin.pytestplugin import (  # noqa
    pytest_sessionstart as sqlalchemy_pytest_sessionstart,
)


def pytest_sessionstart(session):
    """Setup connection string to SQLite Cloud with teest apikey."""

    load_dotenv(find_dotenv(filename="../.env", raise_error_if_not_found=True))

    connection_string = os.getenv("SQLITE_CONNECTION_STRING")
    apikey = os.getenv("SQLITE_API_KEY")
    connection_string = connection_string.replace(
        "sqlitecloud://", "sqlite+sqlitecloud://"
    )

    connection_string += f"?apikey={apikey}"

    session.config.option.dburi = [connection_string]

    sqlalchemy_pytest_sessionstart(session)
