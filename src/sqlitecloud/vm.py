import ctypes

from sqlitecloud.driver import (
    SQCloudConnect,
    SQCloudVM,
    SQCloudVMCompile,
    SQCloudVMStep,
    SQCloudResult,
    SQCloudVMResult,
    SQCloudVMClose,
    SQCloudVMErrorMsg,
    SQCloudVMErrorCode,
    SQCloudVMIsReadOnly,
    SQCloudVMIsExplain,
    SQCloudVMIsFinalized,
    SQCloudVMBindParameterCount,
    SQCloudVMBindParameterIndex,
    SQCloudVMBindParameterName,
    SQCloudVMColumnCount,
    SQCloudVMBindDouble,
    SQCloudVMBindInt,
    SQCloudVMBindInt64,
    SQCloudVMBindNull,
    SQCloudVMBindText,
    SQCloudVMBindBlob,
    SQCloudVMColumnBlob,
    SQCloudVMColumnText,
    SQCloudVMColumnDouble,
    SQCloudVMColumnInt32,
    SQCloudVMColumnInt64,
    SQCloudVMColumnLen,
    SQCloudVMColumnType,
    SQCloudVMLastRowID,
    SQCloudVMChanges,
    SQCloudVMTotalChanges
)
from sqlitecloud.wrapper_types import SQCLOUD_VALUE_TYPE


def compile_vm(conn: SQCloudConnect, query: str) -> SQCloudVM:
    vm = SQCloudVMCompile(conn, ctypes.c_char_p(query.encode("utf-8")), -1, None)

    return vm


def step_vm(vm: SQCloudVMCompile) -> int:
    return SQCloudVMStep(vm)


def result_vm(vm: SQCloudVMCompile) -> SQCloudResult:
    return SQCloudVMResult(vm)


def close_vm(vm: SQCloudVMCompile) -> bool:
    return SQCloudVMClose(vm)


def error_msg_vm(vm: SQCloudVMCompile) -> str | None:
    result = SQCloudVMErrorMsg(vm)

    if result is None:
        return None

    return ctypes.string_at(result).decode('utf-8')


def error_code_vm(vm: SQCloudVMCompile) -> int:
    return SQCloudVMErrorCode(vm)


def is_read_only_vm(vm: SQCloudVMCompile) -> bool:
    return SQCloudVMIsReadOnly(vm)


def is_explain_vm(vm: SQCloudVMCompile) -> int:
    return SQCloudVMIsExplain(vm)


def is_finalized_vm(vm: SQCloudVMCompile) -> bool:
    return SQCloudVMIsFinalized(vm)


def bind_parameter_count_vm(vm: SQCloudVMCompile) -> int:
    return SQCloudVMBindParameterCount(vm)


def bind_parameter_index_vm(vm: SQCloudVMCompile, parameter_name: str) -> int:
    return SQCloudVMBindParameterIndex(vm, ctypes.c_char_p(parameter_name.encode("utf-8")))


def bind_parameter_name_vm(vm: SQCloudVMCompile, index: int) -> str | None:
    result = SQCloudVMBindParameterName(vm, index)

    if result is None:
        return None

    return ctypes.string_at(result).decode('utf-8')


def column_count_vm(vm: SQCloudVMCompile) -> int:
    return SQCloudVMColumnCount(vm)


def bind_double_vm(vm: SQCloudVMCompile, index: int, value: float) -> bool:
    return SQCloudVMBindDouble(vm, index, ctypes.c_double(value))


def bind_int_vm(vm: SQCloudVMCompile, index: int, value: int) -> bool:
    return SQCloudVMBindInt(vm, index, value)


def bind_int64_vm(vm: SQCloudVMCompile, index: int, value: int) -> bool:
    return SQCloudVMBindInt64(vm, index, ctypes.c_int64(value))


def bind_null_vm(vm: SQCloudVMCompile, index: int) -> bool:
    return SQCloudVMBindNull(vm, index)


def bind_text_vm(vm: SQCloudVMCompile, index: int, value: str) -> bool:
    return SQCloudVMBindText(
        vm,
        index,
        ctypes.c_char_p(value.encode('utf-8')),
        len(value.encode('utf-8'))
        )


def bind_blob_vm(vm: SQCloudVMCompile, index: int, value) -> bool:
    return SQCloudVMBindBlob(
        vm,
        index,
        value,
        len(value.encode('utf-8'))
    )


def column_type_vm(vm: SQCloudVMCompile, index: int) -> SQCLOUD_VALUE_TYPE:
    return SQCloudVMColumnType(vm, index)


def column_blob_vm(vm: SQCloudVMCompile, index: int) -> str:
    len_holder = ctypes.c_uint32()

    blob_pointer = SQCloudVMColumnBlob(vm, index, ctypes.byref(len_holder))

    blob_size = len_holder.value
    blob_data = ctypes.cast(blob_pointer, ctypes.POINTER(ctypes.c_ubyte * blob_size)).contents

    return bytes(blob_data[:blob_size]).decode('utf-8')


def column_text_vm(vm: SQCloudVMCompile, index: int) -> str:
    len_holder = ctypes.c_uint32()

    value = SQCloudVMColumnText(vm, index, ctypes.byref(len_holder))

    text_value = value.decode('utf-8') if value else None

    return text_value[:len_holder.value]


def column_double_vm(vm: SQCloudVMCompile, index: int) -> float:
    return SQCloudVMColumnDouble(vm, index)


def column_int_32_vm(vm: SQCloudVMCompile, index: int) -> int:
    return SQCloudVMColumnInt32(vm, index)


def column_int_64_vm(vm: SQCloudVMCompile, index: int) -> int:
    return SQCloudVMColumnInt64(vm, index)


def column_len_vm(vm: SQCloudVMCompile, index: int) -> int:
    return SQCloudVMColumnLen(vm, index)


def last_row_id_vm(vm: SQCloudVMCompile) -> int:
    return SQCloudVMLastRowID(vm)


def changes_vm(vm: SQCloudVMCompile) -> int:
    return SQCloudVMChanges(vm)


def total_changes_vm(vm: SQCloudVMCompile) -> int:
    return SQCloudVMTotalChanges(vm)
