from typing import Any, Callable
from sqlitecloud.driver import SQCloudConnect, SQCloudSetPubSubCallback,SQCloudPubSubCB
from sqlitecloud.wrapper_types import SQCloudResult


SQCloudPubSubCallback = Callable[[SQCloudConnect,SQCloudResult,Any],None]

def subscribe_pub_sub(connection:SQCloudConnect,pub_sub_callback:SQCloudPubSubCallback):
    SQCloudSetPubSubCallback(connection, SQCloudPubSubCB(pub_sub_callback), None)