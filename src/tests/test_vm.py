from sqlitecloud.client import SqliteCloudClient, SqliteCloudAccount
from sqlitecloud.conn_info import user, password, host, db_name, port
from sqlitecloud.vm import compile_vm, step_vm, result_vm


def test_compile_vm():
    account = SqliteCloudAccount(user, password, host, db_name, port)
    client = SqliteCloudClient(cloud_account=account)
    conn = client.open_connection()
    compile_vm(conn, "INSERT INTO employees (emp_name) VALUES (?1);")
    client.disconnect(conn)


def test_step_vm():
    account = SqliteCloudAccount(user, password, host, db_name, port)
    client = SqliteCloudClient(cloud_account=account)
    conn = client.open_connection()
    vm = compile_vm(conn, "INSERT INTO employees (emp_name) VALUES (?1);")
    result = step_vm(vm)
    client.disconnect(conn)

    assert isinstance(result, int), type(result)


def test_result_vm():
    account = SqliteCloudAccount(user, password, host, db_name, port)
    client = SqliteCloudClient(cloud_account=account)
    conn = client.open_connection()
    vm = compile_vm(conn, "INSERT INTO employees (emp_name) VALUES (?1);")
    result = result_vm(vm)
    client.disconnect(conn)

    assert isinstance(result, int), type(result)
