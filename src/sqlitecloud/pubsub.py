from typing import Callable, Optional

from sqlitecloud.datatypes import SQLITECLOUD_PUBSUB_SUBJECT, SQLiteCloudConnect
from sqlitecloud.driver import Driver
from sqlitecloud.resultset import SQLiteCloudResultSet


class SQLiteCloudPubSub:
    def __init__(self) -> None:
        self._driver = Driver()

    def listen(
        self,
        connection: SQLiteCloudConnect,
        subject_type: SQLITECLOUD_PUBSUB_SUBJECT,
        subject_name: str,
        callback: Callable[
            [SQLiteCloudConnect, Optional[SQLiteCloudResultSet], Optional[any]], None
        ],
        data: Optional[any] = None,
    ) -> None:
        subject = "TABLE " if subject_type.value == "TABLE" else ""

        connection.pubsub_callback = callback
        connection.pubsub_data = data

        self._driver.execute(f"LISTEN {subject}{subject_name};", connection)

    def unlisten(
        self,
        connection: SQLiteCloudConnect,
        subject_type: SQLITECLOUD_PUBSUB_SUBJECT,
        subject_name: str,
    ) -> None:
        subject = "TABLE " if subject_type.value == "TABLE" else ""

        self._driver.execute(f"UNLISTEN {subject}{subject_name};", connection)

        connection.pubsub_callback = None
        connection.pubsub_data = None

    def create_channel(
        self, connection: SQLiteCloudConnect, name: str, if_not_exists: bool = False
    ) -> None:
        if if_not_exists:
            self._driver.execute(f"CREATE CHANNEL {name} IF NOT EXISTS;", connection)
        else:
            self._driver.execute(f"CREATE CHANNEL {name};", connection)

    def notify_channel(
        self, connection: SQLiteCloudConnect, name: str, data: str
    ) -> None:
        self._driver.execute(f"NOTIFY {name} '{data}';", connection)

    def set_pubsub_only(self, connection: SQLiteCloudConnect) -> None:
        """
        Close the main socket, leaving only the pub/sub socket opened and ready
        to receive incoming notifications from subscripted channels and tables.

        Connection is no longer able to send commands.
        """
        self._driver.execute("PUBSUB ONLY;", connection)
        self._driver.disconnect(connection, only_main_socket=True)

    def is_connected(self, connection: SQLiteCloudConnect) -> bool:
        return self._driver.is_connected(connection, False)

    def list_connections(self, connection: SQLiteCloudConnect) -> SQLiteCloudResultSet:
        return SQLiteCloudResultSet(
            self._driver.execute("LIST PUBSUB CONNECTIONS;", connection)
        )
