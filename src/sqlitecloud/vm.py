import ctypes

from experiments.test_ctypes import SQCloudConnect
from sqlitecloud.driver import SQCloudVM, SQCloudVMCompile


def compile_vm(conn: SQCloudConnect, query: str) -> SQCloudVM:
    return SQCloudVMCompile(conn, ctypes.c_char_p(query.encode("utf-8")), -1, None)
