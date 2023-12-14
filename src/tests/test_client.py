from sqlitecloud.client import SqliteCloudClient, SqliteCloudAccount
from sqlitecloud.conn_info import user, password, host, db_name, port

# Mocking SQCloudConnect and other dependencies would be necessary for more comprehensive testing.


def test_sqlite_cloud_client_exec_query():
    account = SqliteCloudAccount(user, password, host, db_name, port)
    client = SqliteCloudClient(cloud_account=account)
    assert client
    conn = client.open_connection()
    query = "select * from employees;"
    result = client.exec_query(query, conn)
    assert result
    first_element = next(result)
    assert len(first_element) == 4
    assert "id" in first_element.keys()
    assert "emp_name" in first_element.keys()
    client.disconnect(conn)


def test_sqlite_cloud_client_exec_array():
    account = SqliteCloudAccount(user, password, host, db_name, port)
    client = SqliteCloudClient(cloud_account=account)
    result = client.exec_statement("select * from employees where id = ?", [1])
    assert result
    first_element = next(result)
    assert len(first_element) == 4


def test_sqlite_cloud_error_query():
    account = SqliteCloudAccount(user, password, host, db_name, port)
    client = SqliteCloudClient(cloud_account=account)
    assert client
    conn = client.open_connection()
    query = "select * from ibiza;"
    is_error = False
    try:
        client.exec_query(query, conn)
        client.disconnect(conn)
    except Exception:
        is_error = True
        client.disconnect(conn)
    assert is_error


def test_sqlite_cloud_float_agg_query():
    account = SqliteCloudAccount(user, password, host, db_name, port)
    client = SqliteCloudClient(cloud_account=account)
    assert client
    conn = client.open_connection()
    query = "GET INFO disk_usage_perc;"
    result = client.exec_query(query, conn)
    assert result
    first_element = next(result)
    assert "result" in first_element.keys()
    print("Float result", first_element)
    client.disconnect(conn)


def test_sqlite_cloud_int_agg_query():
    account = SqliteCloudAccount(user, password, host, db_name, port)
    client = SqliteCloudClient(cloud_account=account)
    assert client
    conn = client.open_connection()
    query = "GET INFO process_id;"
    result = client.exec_query(query, conn)
    assert result
    first_element = next(result)
    print("Int result", first_element)
    assert "result" in first_element.keys()
    client.disconnect(conn)
