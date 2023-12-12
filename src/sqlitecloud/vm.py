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
    SQCloudVMBindParameterIndex, SQCloudVMBindParameterName, SQCloudVMColumnCount
)


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
