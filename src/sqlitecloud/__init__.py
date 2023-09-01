from sqlitecloud.client import SqliteCloudClient


def get_sqlitecloud_client(connection_str: str) -> SqliteCloudClient:
    print(connection_str)
