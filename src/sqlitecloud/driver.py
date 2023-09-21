import os
import ctypes

from sqlitecloud.wrapper_types import SQCloudConfig, SQCloudResult

lib_path = os.getenv("SQLITECLOUD_DRIVER_PATH", "./libsqcloud.so")
print(lib_path)
lib = ctypes.CDLL(lib_path)
connect = lib.SQCloudConnect

SQCloudConnect = lib.SQCloudConnect
SQCloudConnect.argtypes = [ctypes.c_char_p, ctypes.c_int, ctypes.POINTER(SQCloudConfig)]
SQCloudConnect.restype = ctypes.c_void_p
SQCloudIsError = lib.SQCloudIsError
SQCloudIsError.argtypes = [
    ctypes.c_void_p
]  # Assuming SQCloudConnection * is a void pointer
SQCloudIsError.restype = ctypes.c_bool

SQCloudErrorMsg = lib.SQCloudErrorMsg
SQCloudErrorMsg.argtypes = [
    ctypes.c_void_p
]  # Assuming SQCloudConnection * is a void pointer
SQCloudErrorMsg.restype = ctypes.c_char_p

SQCloudExec = lib.SQCloudExec
SQCloudExec.argtypes = [
    ctypes.c_void_p,
    ctypes.c_char_p,
]  # Assuming SQCloudConnection * is a void pointer
SQCloudExec.restype = ctypes.POINTER(SQCloudResult)

SQCloudConnectWithString = lib.SQCloudConnectWithString

SQCloudDisconnect = lib.SQCloudDisconnect
SQCloudDisconnect.argtypes = [
    ctypes.c_void_p
]  # Assuming SQCloudConnection * is a void pointer
SQCloudDisconnect.restype = None

SQCloudResultIsOK = lib.SQCloudResultIsOK
SQCloudResultIsOK.argtypes = [
    ctypes.POINTER(SQCloudResult)
]  # Assuming SQCloudResult * is a pointer to void pointer
SQCloudResultIsOK.restype = ctypes.c_bool

SQCloudResultIsError = lib.SQCloudResultIsOK
SQCloudResultIsError.argtypes = [
    ctypes.POINTER(SQCloudResult)
]  # Assuming SQCloudResult * is a pointer to void pointer
SQCloudResultIsError.restype = ctypes.c_bool
SQCloudRowsetCols = lib.SQCloudRowsetCols
SQCloudRowsetCols.argtypes = [ctypes.POINTER(SQCloudResult)]  # Assuming SQCloudResult * is a pointer to void pointer
SQCloudRowsetCols.restype = ctypes.c_uint32

SQCloudRowsetRows = lib.SQCloudRowsetRows
SQCloudRowsetRows.argtypes = [ctypes.POINTER(SQCloudResult)]  # Assuming SQCloudResult * is a pointer to void pointer
SQCloudRowsetRows.restype = ctypes.c_uint32

_SQCloudRowsetColumnName = lib.SQCloudRowsetColumnName
_SQCloudRowsetColumnName.argtypes = [
    ctypes.POINTER(SQCloudResult),  # SQCloudResult *result
    ctypes.c_uint32,  # uint32_t col
    ctypes.POINTER(ctypes.c_uint32)  # uint32_t *len
]
_SQCloudRowsetColumnName.restype = ctypes.c_char_p
def SQCloudRowsetColumnName(result_set, col_n):
    name_len = ctypes.c_uint32()
    col_name = _SQCloudRowsetColumnName(result_set,col_n,ctypes.byref(name_len))
    print("name_len",name_len.value, col_name.decode('utf-8'))
    return col_name
