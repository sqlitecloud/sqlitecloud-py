import pytest

from sqlitecloud.client import SqliteCloudClient, SqliteCloudAccount
from sqlitecloud.conn_info import user, password, host, db_name, port
from sqlitecloud.vm import (
    compile_vm,
    step_vm,
    result_vm,
    close_vm,
    error_msg_vm,
    error_code_vm,
    is_read_only_vm,
    is_explain_vm,
    is_finalized_vm,
    bind_parameter_count_vm,
    bind_parameter_index_vm,
    bind_parameter_name_vm,
    column_count_vm
)


@pytest.fixture(autouse=True)
def get_client():
    account = SqliteCloudAccount(user, password, host, db_name, port)
    client = SqliteCloudClient(cloud_account=account)

    return client


# FIXTURE EXAMPLE
@pytest.fixture(autouse=True)
def get_client_v2():
    account = SqliteCloudAccount(user, password, host, db_name, port)
    client = SqliteCloudClient(cloud_account=account)

    conn = client.open_connection()

    try:
        yield conn
    finally:
        client.disconnect(conn)


def test_compile_vm(get_client):
    client = get_client
    conn = client.open_connection()
    compile_vm(conn, "INSERT INTO employees (emp_name) VALUES (?1);")
    client.disconnect(conn)


def test_step_vm(get_client):
    client = get_client
    conn = client.open_connection()
    vm = compile_vm(conn, "INSERT INTO employees (emp_name) VALUES (?1);")
    result = step_vm(vm)
    client.disconnect(conn)

    assert isinstance(result, int), type(result)


def test_result_vm(get_client):
    client = get_client
    conn = client.open_connection()
    vm = compile_vm(conn, "INSERT INTO employees (emp_name) VALUES (?1);")
    result = result_vm(vm)
    client.disconnect(conn)

    assert isinstance(result, int), type(result)


def test_close_vm(get_client):
    client = get_client
    conn = client.open_connection()
    vm = compile_vm(conn, "INSERT INTO employees (emp_name) VALUES (?1);")
    res = close_vm(vm)

    assert res is True


def test_error_msg_vm(get_client):
    client = get_client
    conn = client.open_connection()
    vm = compile_vm(conn, "INSERT INTO employees (emp_name) VALUES (?1);")
    res = error_msg_vm(vm)

    assert res is None


def test_error_code_vm(get_client):
    client = get_client
    conn = client.open_connection()
    vm = compile_vm(conn, "INSERT INTO employees (emp_name) VALUES (?1);")
    res = error_code_vm(vm)

    assert res == 0


def test_vm_is_not_read_only(get_client):
    client = get_client
    conn = client.open_connection()
    vm = compile_vm(conn, "INSERT INTO employees (emp_name) VALUES (?1);")
    res = is_read_only_vm(vm)
    client.disconnect(conn)

    assert res is False


def test_vm_is_read_only(get_client):
    client = get_client
    conn = client.open_connection()
    vm = compile_vm(conn, "SELECT * FROM employees")
    res = is_read_only_vm(vm)
    client.disconnect(conn)

    assert res is True


def test_vm_is_explain(get_client):
    client = get_client
    conn = client.open_connection()
    vm = compile_vm(conn, "EXPLAIN INSERT INTO employees (emp_name) VALUES (?1);")
    res = is_explain_vm(vm)
    client.disconnect(conn)

    assert res == 1


def test_vm_is_not_finalized(get_client):
    client = get_client
    conn = client.open_connection()
    vm = compile_vm(conn, "INSERT INTO employees (emp_name) VALUES (?1);")
    res = is_finalized_vm(vm)
    client.disconnect(conn)

    assert res is False


def test_vm_bin_parameter_count(get_client):
    client = get_client
    conn = client.open_connection()
    vm = compile_vm(conn, "INSERT INTO employees (emp_name) VALUES (?1);")
    step_vm(vm)
    res = bind_parameter_count_vm(vm)
    client.disconnect(conn)

    assert res == 1


def test_bind_parameter_index_vm(get_client):
    client = get_client
    conn = client.open_connection()
    vm = compile_vm(conn, "INSERT INTO employees (emp_name) VALUES (?1);")
    step_vm(vm)
    res = bind_parameter_index_vm(vm=vm, parameter_name='1')
    client.disconnect(conn)

    assert res == 0


def test_bind_parameter_name_vm(get_client):
    client = get_client
    conn = client.open_connection()
    vm = compile_vm(conn, "INSERT INTO employees (emp_name) VALUES (?1);")
    step_vm(vm)
    res = bind_parameter_name_vm(vm=vm, index=1)
    client.disconnect(conn)

    assert res == '?1'


def test_column_count_vm(get_client):
    client = get_client
    conn = client.open_connection()
    vm = compile_vm(conn, "SELECT * FROM employees LIMIT1;")
    step_vm(vm)
    res = column_count_vm(vm)
    client.disconnect(conn)

    assert res == 2
