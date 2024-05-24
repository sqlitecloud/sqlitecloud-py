from typing import Callable, Optional

from sqlitecloud.driver import Driver
from sqlitecloud.resultset import SqliteCloudResultSet
from sqlitecloud.types import SQCLOUD_PUBSUB_SUBJECT, SQCloudConnect


class SqliteCloudPubSub:
    def __init__(self) -> None:
        self._driver = Driver()

    def listen(
        self,
        connection: SQCloudConnect,
        subject_type: SQCLOUD_PUBSUB_SUBJECT,
        subject_name: str,
        callback: Callable[
            [SQCloudConnect, Optional[SqliteCloudResultSet], Optional[any]], None
        ],
        data: Optional[any] = None,
    ) -> None:
        subject = "TABLE " if subject_type.value == "TABLE" else ""

        connection.pubsub_callback = callback
        connection.pubsub_data = data

        self._driver.execute(f"LISTEN {subject}{subject_name};", connection)

    def unlisten(
        self,
        connection: SQCloudConnect,
        subject_type: SQCLOUD_PUBSUB_SUBJECT,
        subject_name: str,
    ) -> None:
        subject = "TABLE " if subject_type.value == "TABLE" else ""

        self._driver.execute(f"UNLISTEN {subject}{subject_name};", connection)

        connection.pubsub_callback = None
        connection.pubsub_data = None

    def create_channel(
        self, connection: SQCloudConnect, name: str, if_not_exists: bool = False
    ) -> None:
        if if_not_exists:
            self._driver.execute(f"CREATE CHANNEL {name} IF NOT EXISTS;", connection)
        else:
            self._driver.execute(f"CREATE CHANNEL {name};", connection)

    def notify_channel(self, connection: SQCloudConnect, name: str, data: str) -> None:
        self._driver.execute(f"NOTIFY {name} '{data}';", connection)

    def set_pubsub_only(self, connection: SQCloudConnect) -> None:
        """
        Close the main socket, leaving only the pub/sub socket opened and ready
        to receive incoming notifications from subscripted channels and tables.

        Connection is no longer able to send commands.
        """
        self._driver.execute("PUBSUB ONLY;", connection)
        self._driver.disconnect(connection, only_main_socket=True)

    def is_connected(self, connection: SQCloudConnect) -> bool:
        return self._driver.is_connected(connection, False)

    def list_connections(self, connection: SQCloudConnect) -> SqliteCloudResultSet:
        return SqliteCloudResultSet(
            self._driver.execute("LIST PUBSUB CONNECTIONS;", connection)
        )
