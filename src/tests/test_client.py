import os
from sqlitecloud.client import SqliteCloudClient, SqliteCloudAccount


# Mocking SQCloudConnect and other dependencies would be necessary for more comprehensive testing.


def test_sqlite_cloud_client_exec_query():
    connection_str = os.getenv("TEST_CONNECTION_URL")
    print("Connection:", connection_str)
    account = SqliteCloudAccount(
        "ADMIN", "F77VNEnVTS", "qrznfgtzm.sqlite.cloud", "people", 8860
    )
    client = SqliteCloudClient(cloud_account=account)
    assert client
    conn = client.open_connection()
    query = "select * from employees;"
    result = client.exec_query(query, conn)
    assert result
    for single_r in result:
        print(single_r["col_1"])
    client.disconnect(conn)
