from sqlitecloud.client import SqliteCloudClient

VERSION = "0.1.2"


def get_sqlitecloud_client(connection_str: str) -> SqliteCloudClient:
    print(connection_str)
