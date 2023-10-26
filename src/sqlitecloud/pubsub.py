import ctypes
from typing import Any, Callable
from sqlitecloud.driver import (
    SQCloudConnect,
    SQCloudExec,
    SQCloudSetPubSubCallback,
    SQCloudPubSubCB,
)
from sqlitecloud.wrapper_types import SQCloudResult

SQCloudPubSubCallback = Callable[[SQCloudConnect, SQCloudResult, Any], None]


def subscribe_pub_sub(
    connection: SQCloudConnect, channel: str, pub_sub_callback: SQCloudPubSubCB
):
    SQCloudSetPubSubCallback(connection, pub_sub_callback, None)
    SQCloudExec(
        connection,
        ctypes.c_char_p((f"CREATE CHANNEL {channel} IF NOT EXISTS;".encode("utf-8"))),
    )
    SQCloudExec(connection, ctypes.c_char_p((f"LISTEN {channel};".encode("utf-8"))))
    print(f"Listening{channel}")
