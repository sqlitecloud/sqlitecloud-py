from sqlitecloud.client import SqliteCloudClient, SqliteCloudAccount
from sqlitecloud.upload import upload_db
from sqlitecloud.conn_info import user,password,host,db_name,port

# Mocking SQCloudConnect and other dependencies would be necessary for more comprehensive testing.


def test_sqlite_upload():
    account = SqliteCloudAccount(user, password, host, db_name, port)
    client = SqliteCloudClient(cloud_account=account)
    conn = client.open_connection()
    upload_db(conn, "demo-test-1", None, "./src/tests/test.db")
    client.disconnect(conn)
