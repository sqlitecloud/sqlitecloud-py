from sqlitecloud.client import SqliteCloudClient
version='0.1.1'

def get_sqlitecloud_client(connection_str: str) -> SqliteCloudClient:
    print(connection_str)
