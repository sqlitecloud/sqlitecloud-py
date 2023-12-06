import ctypes

from sqlitecloud.driver import SQCloudConnect, SQCloudVM, SQCloudVMCompile, SQCloudVMStep, SQCloudResult, SQCloudVMResult


def compile_vm(conn: SQCloudConnect, query: str) -> SQCloudVM:
    return SQCloudVMCompile(conn, ctypes.c_char_p(query.encode("utf-8")), -1, None)


def step_vm(vm: SQCloudVMCompile) -> int:
    return SQCloudVMStep(vm)


def result_vm(vm: SQCloudVMCompile) -> SQCloudResult:
    return SQCloudVMResult(vm)
