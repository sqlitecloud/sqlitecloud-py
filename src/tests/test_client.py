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
    first_element = next(result)
    assert len(first_element) == 2
    assert "emp_id" in first_element.keys()
    assert "emp_name" in first_element.keys()
    client.disconnect(conn)
