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
    query = "CREATE TABLE IF NOT EXISTS employees (emp_id INT, emp_name CHAR );"
    result = client.exec_query(query, conn)
    query = "select emp_id from employees;"
    result = client.exec_query(query, conn)
    assert result
    for single_r in result:
        print(single_r)
    client.disconnect(conn)

    # You would need to mock the _open_connection method and SQCloudConnect for more realistic testing.

    # result = client.exec_query(query)
    # assert result
