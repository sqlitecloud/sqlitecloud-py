from sqlitecloud.client import SqliteCloudClient

0.1.3


def get_sqlitecloud_client(connection_str: str) -> SqliteCloudClient:
    print(connection_str)
