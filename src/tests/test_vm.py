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
    column_count_vm,
    bind_double_vm,
    bind_null_vm,
    bind_text_vm,
    bind_blob_vm,
    column_blob_vm,
    column_text_vm,
    column_double_vm,
    column_int_32_vm,
    column_int_64_vm,
    column_len_vm,
    column_type_vm,
    last_row_id_vm,
    changes_vm, total_changes_vm
)
from sqlitecloud.wrapper_types import SQCLOUD_VALUE_TYPE


@pytest.fixture()
def get_conn():
    account = SqliteCloudAccount(user, password, host, db_name, port)
    client = SqliteCloudClient(cloud_account=account)

    conn = client.open_connection()

    try:
        yield conn
    finally:
        client.disconnect(conn)


def test_compile_vm(get_conn):
    conn = get_conn
    compile_vm(conn, "INSERT INTO employees (emp_name) VALUES (?1);")


def test_step_vm(get_conn):
    conn = get_conn
    vm = compile_vm(conn, "INSERT INTO employees (emp_name) VALUES (?1);")
    result = step_vm(vm)

    assert isinstance(result, int), type(result)


def test_result_vm(get_conn):
    conn = get_conn
    vm = compile_vm(conn, "INSERT INTO employees (emp_name) VALUES (?1);")
    result = result_vm(vm)

    assert isinstance(result, int), type(result)


def test_close_vm(get_conn):
    conn = get_conn
    vm = compile_vm(conn, "INSERT INTO employees (emp_name) VALUES (?1);")
    res = close_vm(vm)
    assert res is True


def test_error_msg_vm(get_conn):
    conn = get_conn
    vm = compile_vm(conn, "INSERT INTO employees (emp_name) VALUES (?1);")
    res = error_msg_vm(vm)
    assert res is None


def test_error_code_vm(get_conn):
    conn = get_conn
    vm = compile_vm(conn, "INSERT INTO employees (emp_name) VALUES (?1);")
    res = error_code_vm(vm)
    assert res == 0


def test_vm_is_not_read_only(get_conn):
    conn = get_conn
    vm = compile_vm(conn, "INSERT INTO employees (emp_name) VALUES (?1);")
    res = is_read_only_vm(vm)
    assert res is False


def test_vm_is_read_only(get_conn):
    conn = get_conn
    vm = compile_vm(conn, "SELECT * FROM employees")
    res = is_read_only_vm(vm)
    assert res is True


def test_vm_is_explain(get_conn):
    conn = get_conn
    vm = compile_vm(conn, "EXPLAIN INSERT INTO employees (emp_name) VALUES (?1);")
    res = is_explain_vm(vm)
    assert res == 1


def test_vm_is_not_finalized(get_conn):
    conn = get_conn
    vm = compile_vm(conn, "INSERT INTO employees (emp_name) VALUES (?1);")
    res = is_finalized_vm(vm)
    assert res is False


def test_vm_bin_parameter_count(get_conn):
    conn = get_conn
    vm = compile_vm(conn, "INSERT INTO employees (emp_name) VALUES (?1);")
    step_vm(vm)
    res = bind_parameter_count_vm(vm)
    assert res == 1


def test_bind_parameter_index_vm(get_conn):
    conn = get_conn
    vm = compile_vm(conn, "INSERT INTO employees (emp_name) VALUES (?1);")
    step_vm(vm)
    res = bind_parameter_index_vm(vm=vm, parameter_name='1')
    assert res == 0


def test_bind_parameter_name_vm(get_conn):
    conn = get_conn
    vm = compile_vm(conn, "INSERT INTO employees (emp_name) VALUES (?1);")
    step_vm(vm)
    res = bind_parameter_name_vm(vm=vm, index=1)
    assert isinstance(res, str)


def test_column_count_vm(get_conn):
    conn = get_conn
    vm = compile_vm(conn, "SELECT * FROM employees LIMIT1;")
    step_vm(vm)
    res = column_count_vm(vm)
    assert res == 4


def test_bind_double_vm(get_conn):
    conn = get_conn
    vm = compile_vm(conn, "INSERT INTO employees (emp_name, salary) VALUES (?1, ?2)")
    res = bind_double_vm(vm=vm, index=2, value=2.340)
    assert res is True


def test_bind_int_vm(get_conn):
    conn = get_conn
    vm = compile_vm(conn, "INSERT INTO employees (emp_name, salary) VALUES (?1, ?2)")
    res = bind_double_vm(vm=vm, index=2, value=2)
    assert res is True


def test_bind_int64_vm(get_conn):
    conn = get_conn
    vm = compile_vm(conn, "INSERT INTO employees (emp_name, salary) VALUES (?1, ?2)")
    res = bind_double_vm(vm=vm, index=2, value=123456789012345)
    assert res is True


def test_bind_null_vm(get_conn):
    conn = get_conn
    vm = compile_vm(conn, "INSERT INTO employees (emp_name, salary) VALUES (?1, ?2)")
    res = bind_null_vm(vm=vm, index=1)
    assert res is True


def test_bind_text_vm(get_conn):
    conn = get_conn
    vm = compile_vm(conn, "INSERT INTO employees (emp_name, salary) VALUES (?1, ?2)")
    res = bind_text_vm(vm=vm, index=1, value='Jonathan')
    assert res is True


def test_bind_blob_vm(get_conn):
    conn = get_conn
    vm = compile_vm(conn, "INSERT INTO employees (emp_name, salary) VALUES (?1, ?2)")
    res = bind_blob_vm(vm=vm, index=1, value='Fake Blob Value')
    assert res is True


def test_column_type_vm(get_conn):
    conn = get_conn
    vm = compile_vm(conn, "SELECT * FROM employees LIMIT 1;")
    step_vm(vm)

    column_type = column_type_vm(vm, 0)

    assert column_type == SQCLOUD_VALUE_TYPE.VALUE_INTEGER


def test_column_blob_vm(get_conn):
    value: str | None = None
    conn = get_conn
    vm = compile_vm(conn, "SELECT * FROM employees LIMIT 1 OFFSET 1;")
    step_vm(vm)

    column_count: int = column_count_vm(vm)

    for index in range(0, column_count):
        column_type = column_type_vm(vm, index)

        match column_type:
            case SQCLOUD_VALUE_TYPE.VALUE_BLOB:
                value = column_blob_vm(vm, index)
                break

    assert isinstance(value, str)
    assert value == '\x01\x02\x03\x04\x05'


def test_column_text_vm(get_conn):
    value: str | None = None
    conn = get_conn
    vm = compile_vm(conn, "SELECT * FROM employees LIMIT 1;")
    step_vm(vm)

    column_count: int = column_count_vm(vm)

    for index in range(0, column_count):
        column_type = column_type_vm(vm, index)

        match column_type:
            case SQCLOUD_VALUE_TYPE.VALUE_TEXT:
                value = column_text_vm(vm, index)
                break
            case _:
                value = None

    assert isinstance(value, str)


def test_column_double_vm(get_conn):
    value: float | None = None
    conn = get_conn
    vm = compile_vm(conn, "SELECT * FROM employees LIMIT 1;")
    step_vm(vm)

    column_count: int = column_count_vm(vm)

    for index in range(0, column_count):
        column_type = column_type_vm(vm, index)

        match column_type:
            case SQCLOUD_VALUE_TYPE.VALUE_FLOAT:
                value: float = column_double_vm(vm, index)
                break
            case _:
                value = None

    assert isinstance(value, float)
    assert value == 18000.0


def test_column_int_32_vm(get_conn):
    value: int | None = None
    conn = get_conn
    vm = compile_vm(conn, "SELECT * FROM employees LIMIT 1;")
    step_vm(vm)

    column_count: int = column_count_vm(vm)

    for index in range(0, column_count):
        column_type = column_type_vm(vm, index)

        match column_type:
            case SQCLOUD_VALUE_TYPE.VALUE_INTEGER:
                value: int = column_int_32_vm(vm, index)
                break
            case _:
                value = None

    assert isinstance(value, int)
    assert value == 1


def test_column_int_64_vm(get_conn):
    value: int | None = None
    conn = get_conn
    vm = compile_vm(conn, "SELECT * FROM employees LIMIT 1;")
    step_vm(vm)

    column_count: int = column_count_vm(vm)

    for index in range(0, column_count):
        column_type = column_type_vm(vm, index)

        match column_type:
            case SQCLOUD_VALUE_TYPE.VALUE_INTEGER:
                value: int = column_int_64_vm(vm, index)
                break
            case _:
                value = None

    assert isinstance(value, int)
    assert value == 1


def test_column_len_vm(get_conn):
    column_content_length: int | None = None
    conn = get_conn
    vm = compile_vm(conn, "SELECT * FROM employees LIMIT 1;")
    step_vm(vm)

    column_count: int = column_count_vm(vm)

    for index in range(0, column_count):
        column_type = column_type_vm(vm, index)

        match column_type:
            case SQCLOUD_VALUE_TYPE.VALUE_TEXT:
                column_content_length = column_len_vm(vm, index)
                break
            case _:
                column_content_length = None

    assert isinstance(column_content_length, int)
    assert column_content_length == 4


def test_last_row_id_vm(get_conn):
    conn = get_conn
    vm = compile_vm(conn, "INSERT INTO employees (emp_name, salary) VALUES (?1, ?2)")
    step_vm(vm)

    row_id = last_row_id_vm(vm)
    assert isinstance(row_id, int)


def test_changes_vm(get_conn):
    conn = get_conn
    vm = compile_vm(conn, "INSERT INTO employees (emp_name, salary) VALUES (?1, ?2)")
    step_vm(vm)
    changes = changes_vm(vm)
    assert changes == 1


def test_total_changes_vm(get_conn):
    conn = get_conn

    vm = compile_vm(conn, "INSERT INTO employees (emp_name, salary) VALUES (?1, ?2)")
    step_vm(vm)

    changes = total_changes_vm(vm)

    assert changes == 1

    vm = compile_vm(conn, "INSERT INTO employees (emp_name, salary) VALUES (?1, ?2)")
    step_vm(vm)

    changes = total_changes_vm(vm)

    assert changes == 2
