import ctypes
import dataclasses
import os
from typing import Any, Callable, List, Type

from sqlitecloud.wrapper_types import SQCLOUD_VALUE_TYPE, SQCloudConfig, SQCloudResult

lib_path = os.getenv(key="SQLITECLOUD_DRIVER_PATH", default="./libsqcloud.so")
print("Loading SQLITECLOUD lib from:", lib_path)
lib = ctypes.CDLL(lib_path)
connect = lib.SQCloudConnect


class SQCloudConnection:
    pass


SQCloudConnect: Callable[
    [str, str, int, SQCloudConfig], SQCloudConnection
] = lib.SQCloudConnect  # self._encode_str_to_c(self.hostname), self.port, self.config
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

SQCloudResultType = lib.SQCloudResultType
SQCloudResultType.argtypes = [ctypes.POINTER(SQCloudResult)]  # SQCloudResult *result
SQCloudResultType.restype = ctypes.c_uint32  # SQCLOUD_RESULT_TYPE return type


SQCloudRowsetCols = lib.SQCloudRowsetCols
SQCloudRowsetCols.argtypes = [
    ctypes.POINTER(SQCloudResult)
]  # Assuming SQCloudResult * is a pointer to void pointer
SQCloudRowsetCols.restype = ctypes.c_uint32

SQCloudRowsetRows = lib.SQCloudRowsetRows
SQCloudRowsetRows.argtypes = [
    ctypes.POINTER(SQCloudResult)
]  # Assuming SQCloudResult * is a pointer to void pointer
SQCloudRowsetRows.restype = ctypes.c_uint32

_SQCloudRowsetColumnName = lib.SQCloudRowsetColumnName
_SQCloudRowsetColumnName.argtypes = [
    ctypes.POINTER(SQCloudResult),  # SQCloudResult *result
    ctypes.c_uint32,  # uint32_t col
    ctypes.POINTER(ctypes.c_uint32),  # uint32_t *len
]
_SQCloudRowsetColumnName.restype = ctypes.c_char_p


def SQCloudRowsetColumnName(result_set, col_n):
    name_len = ctypes.c_uint32()
    col_name = _SQCloudRowsetColumnName(result_set, col_n, ctypes.byref(name_len))
    # print("name_len",name_len.value, col_name.decode('utf-8'))
    return col_name.decode("utf-8")[0 : name_len.value]


SQCloudRowsetValueType = lib.SQCloudRowsetValueType
SQCloudRowsetValueType.argtypes = [
    ctypes.POINTER(SQCloudResult),  # SQCloudResult *result
    ctypes.c_uint32,  # uint32_t row
    ctypes.c_uint32,  # uint32_t col
]
SQCloudRowsetValueType.restype = SQCLOUD_VALUE_TYPE


SQCloudRowsetInt32Value = lib.SQCloudRowsetInt32Value
SQCloudRowsetInt32Value.argtypes = [
    ctypes.POINTER(SQCloudResult),  # SQCloudResult *result
    ctypes.c_uint32,  # uint32_t row
    ctypes.c_uint32,  # uint32_t col
]
SQCloudRowsetInt32Value.restype = ctypes.c_int32  # int32_t return type

SQCloudRowsetInt64Value = lib.SQCloudRowsetInt64Value
SQCloudRowsetInt64Value.argtypes = [
    ctypes.POINTER(SQCloudResult),  # SQCloudResult *result
    ctypes.c_uint32,  # uint32_t row
    ctypes.c_uint32,  # uint32_t col
]
SQCloudRowsetInt64Value.restype = ctypes.c_int32  # int32_t return type

SQCloudRowsetFloatValue = lib.SQCloudRowsetFloatValue
SQCloudRowsetFloatValue.argtypes = [
    ctypes.POINTER(SQCloudResult),  # SQCloudResult *result
    ctypes.c_uint32,  # uint32_t row
    ctypes.c_uint32,  # uint32_t col
]
SQCloudRowsetFloatValue.restype = ctypes.c_float  # int32_t return type


# Define the function signature
_SQCloudRowsetValue = lib.SQCloudRowsetValue
_SQCloudRowsetValue.argtypes = [
    ctypes.POINTER(SQCloudResult),  # SQCloudResult *result
    ctypes.c_uint32,  # uint32_t row
    ctypes.c_uint32,  # uint32_t col
    ctypes.POINTER(ctypes.c_uint32),  # uint32_t *len
]
_SQCloudRowsetValue.restype = ctypes.c_char_p


def SQCloudRowsetValue(result_set, row, col):
    value_len = ctypes.c_uint32()
    data = _SQCloudRowsetValue(result_set, row, col, ctypes.byref(value_len))
    return data[0 : value_len.value]


_SQCloudExecArray = lib.SQCloudExecArray
_SQCloudExecArray.argtypes = [
    ctypes.c_void_p,  # SQCloudConnection *connection
    ctypes.c_char_p,  # const char *command
    ctypes.POINTER(ctypes.c_char_p),  # const char **values
    ctypes.POINTER(ctypes.c_uint32),  # uint32_t len[]
    ctypes.POINTER(ctypes.c_uint32),  # SQCLOUD_VALUE_TYPE types[]
    ctypes.c_uint32,  # uint32_t n
]

_SQCloudExecArray.restype = ctypes.POINTER(SQCloudResult)  # SQCloudResult * return type


def _envinc_type(value: Any) -> int:
    if not isinstance(value, (float, int, str)):
        raise Exception("Invalid type parameter " + type(value))
    print("ev type:", str(type(value)))
    match str(type(value)):
        case "str":
            return SQCLOUD_VALUE_TYPE.VALUE_TEXT
        case "<class 'int'>":
            return SQCLOUD_VALUE_TYPE.VALUE_INTEGER
        case "float":
            return SQCLOUD_VALUE_TYPE.VALUE_FLOAT
    return SQCLOUD_VALUE_TYPE.VALUE_NULL


@dataclasses.dataclass
class SqlParameter:
    byte_value: ctypes.c_char_p
    py_value: Type


def SQCloudExecArray(
    conn: SQCloudConnect, query: ctypes.c_char_p, values: List[SqlParameter]
) -> SQCloudResult:
    n = len(values)
    b_values = [v.byte_value for v in values]
    lengths = [len(val.byte_value.value) for val in values]
    types = list(ctypes.c_uint32(_envinc_type(v.py_value)) for v in values)
    result_ptr = _SQCloudExecArray(
        conn,
        query,
        (ctypes.c_char_p * n)(*b_values),
        (ctypes.c_uint32 * n)(*lengths),
        (ctypes.c_uint32 * n)(*types),
        ctypes.c_uint32(n),
    )
    return result_ptr


SQCloudResultFree = lib.SQCloudResultFree
SQCloudResultFree.argtypes = [ctypes.POINTER(SQCloudResult)]  # SQCloudResult *result
SQCloudResultFree.restype = None

SQCloudResultFloat = lib.SQCloudResultFloat
SQCloudResultFloat.argtypes = [ctypes.POINTER(SQCloudResult)]  # SQCloudResult *result
SQCloudResultFloat.restype = ctypes.c_float  # float return type

SQCloudResultInt32 = lib.SQCloudResultInt32
SQCloudResultInt32.argtypes = [ctypes.POINTER(SQCloudResult)]  # SQCloudResult *result
SQCloudResultInt32.restype = ctypes.c_int32  # int32_t return type

SQCloudResultDump = lib.SQCloudResultDump
SQCloudResultDump.argtypes = [
    ctypes.c_void_p,  # SQCloudConnection *connection
    ctypes.POINTER(SQCloudResult),  # SQCloudResult *result
]
SQCloudResultDump.restype = None

CallbackFunc = ctypes.CFUNCTYPE(
    ctypes.c_int,
    ctypes.c_void_p,
    ctypes.c_void_p,
    ctypes.POINTER(ctypes.c_uint32),
    ctypes.c_int64,
    ctypes.c_int64,
)

SQCloudUploadDatabase = lib.SQCloudUploadDatabase
SQCloudUploadDatabase.argtypes = [
    ctypes.c_void_p,  # SQCloudConnection *connection
    ctypes.c_char_p,  # const char *dbname
    ctypes.c_char_p,  # const char *key
    ctypes.c_void_p,  # void *xdata
    ctypes.c_int64,  # int64_t dbsize
    CallbackFunc,  # int (*xCallback)(void *xdata, void *buffer, uint32_t *blen, int64_t ntot, int64_t nprogress)
]
SQCloudUploadDatabase.restype = ctypes.c_int  # Return type


# Define the SQCloudPubSubCB function signature
SQCloudPubSubCB = ctypes.CFUNCTYPE(
    ctypes.c_void_p, ctypes.POINTER(SQCloudResult), ctypes.c_void_p
)


# Define the function signature
SQCloudSetPubSubCallback = lib.SQCloudSetPubSubCallback
SQCloudSetPubSubCallback.argtypes = [
    ctypes.c_void_p,  # SQCloudConnection *connection
    SQCloudPubSubCB,  # SQCloudPubSubCB callback
    ctypes.c_void_p,  # void *data
]
SQCloudSetPubSubCallback.restype = None


class SQCloudVM(ctypes.Structure):
    pass


SQCloudVMCompile = lib.SQCloudVMCompile

SQCloudVMCompile.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_int32, ctypes.c_void_p]
SQCloudVMCompile.restype = ctypes.POINTER(SQCloudVM)
