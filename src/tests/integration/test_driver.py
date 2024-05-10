import os
from sqlitecloud.client import SqliteCloudClient
from sqlitecloud.driver import Driver, SQCloudConnect
from sqlitecloud.types import SQCloudConfig, SqliteCloudAccount


# class TestDriver:
#     def test_internal_socket_read_empty_stream(self):
#         driver = Driver()

#         config = SQCloudConfig()
#         config.account = SqliteCloudAccount()
#         config.account.username = os.getenv("SQLITE_USER")
#         config.account.password = os.getenv("SQLITE_PASSWORD")

#         conn = driver.connect("nejvjtcliz.sqlite.cloud", 8860, config)
#         assert isinstance(conn, SQCloudConnect)

#         buffer = driver._internal_socket_read(conn)
#         assert buffer == ""
