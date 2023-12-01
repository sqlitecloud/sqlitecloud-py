import ctypes

from sqlitecloud.driver import SQCloudConnect, SQCloudVM, SQCloudVMCompile, SQCloudVMStep


def compile_vm(conn: SQCloudConnect, query: str) -> SQCloudVM:
    return SQCloudVMCompile(conn, ctypes.c_char_p(query.encode("utf-8")), -1, None)


def step_vm(vm: SQCloudVMCompile) -> int:
    return SQCloudVMStep(vm)
