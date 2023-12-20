import ctypes
import dataclasses
import os
from typing import Any, Callable, List, Type

from dotenv import load_dotenv

from sqlitecloud.wrapper_types import SQCLOUD_VALUE_TYPE, SQCloudConfig, SQCloudResult

load_dotenv()

lib_path = os.getenv(key="SQLITECLOUD_DRIVER_PATH", default="./libsqcloud.so")
print("Loading SQLITECLOUD lib from:", lib_path)
lib = ctypes.CDLL(lib_path)
connect = lib.SQCloudConnect


class SQCloudConnection(ctypes.Structure):
    pass


SQCloudConnect: Callable[
    [str, str, int, SQCloudConfig], SQCloudConnection
] = lib.SQCloudConnect
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


# Define the SQCloudVMCompile signature
SQCloudVMCompile = lib.SQCloudVMCompile
SQCloudVMCompile.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_int32, ctypes.c_void_p]
SQCloudVMCompile.restype = ctypes.POINTER(SQCloudVM)

# Define the SQCloudVMStep signature
SQCloudVMStep = lib.SQCloudVMStep
SQCloudVMStep.argtypes = [ctypes.POINTER(SQCloudVM)]
SQCloudVMStep.restype = ctypes.c_int8

# Define the SQCloudVMResult signature
SQCloudVMResult = lib.SQCloudVMResult
SQCloudVMResult.argtypes = [ctypes.POINTER(SQCloudVM)]
SQCloudVMResult.restype = ctypes.c_void_p


# Define the SQCloudVMClose signature
SQCloudVMClose = lib.SQCloudVMClose
SQCloudVMClose.argtypes = [ctypes.POINTER(SQCloudVM)]
SQCloudVMClose.restype = ctypes.c_bool


# Define the SQCloudVMErrorMsg signature
SQCloudVMErrorMsg = lib.SQCloudVMErrorMsg
SQCloudVMErrorMsg.argtypes = [ctypes.POINTER(SQCloudVM)]
SQCloudVMErrorMsg.restype = ctypes.c_char_p


# Define the SQCloudVMErrorCode signature
SQCloudVMErrorCode = lib.SQCloudVMErrorCode
SQCloudVMErrorCode.argtypes = [ctypes.POINTER(SQCloudVM)]
SQCloudVMErrorCode.restype = ctypes.c_int


# Define the SQCloudVMIsReadOnly signature
SQCloudVMIsReadOnly = lib.SQCloudVMIsReadOnly
SQCloudVMIsReadOnly.argtypes = [ctypes.POINTER(SQCloudVM)]
SQCloudVMIsReadOnly.restype = ctypes.c_bool


# Define the SQCloudVMIsExplain signature
SQCloudVMIsExplain = lib.SQCloudVMIsExplain
SQCloudVMIsExplain.argtypes = [ctypes.POINTER(SQCloudVM)]
SQCloudVMIsExplain.restype = ctypes.c_int


# Define the SQCloudVMIsFinalized signature
SQCloudVMIsFinalized = lib.SQCloudVMIsFinalized
SQCloudVMIsFinalized.argtypes = [ctypes.POINTER(SQCloudVM)]
SQCloudVMIsFinalized.restype = ctypes.c_bool


# Define the SQCloudVMBindParameterCount signature
SQCloudVMBindParameterCount = lib.SQCloudVMBindParameterCount
SQCloudVMBindParameterCount.argtypes = [ctypes.POINTER(SQCloudVM)]
SQCloudVMBindParameterCount.restype = ctypes.c_int


# Define the SQCloudVMBindParameterIndex signature
SQCloudVMBindParameterIndex = lib.SQCloudVMBindParameterIndex
SQCloudVMBindParameterIndex.argtypes = [ctypes.POINTER(SQCloudVM), ctypes.c_char_p]
SQCloudVMBindParameterIndex.restype = ctypes.c_int


# Define the SQCloudVMBindParameterName signature
SQCloudVMBindParameterName = lib.SQCloudVMBindParameterName
SQCloudVMBindParameterName.argtypes = [ctypes.POINTER(SQCloudVM), ctypes.c_int]
SQCloudVMBindParameterName.restype = ctypes.c_char_p


# Define the SQCloudVMColumnCount signature
SQCloudVMColumnCount = lib.SQCloudVMColumnCount
SQCloudVMColumnCount.argtypes = [ctypes.POINTER(SQCloudVM)]
SQCloudVMColumnCount.restype = ctypes.c_int


# Define the SQCloudVMBindDouble signature
SQCloudVMBindDouble = lib.SQCloudVMBindDouble
SQCloudVMBindDouble.argtypes = [ctypes.POINTER(SQCloudVM), ctypes.c_int, ctypes.c_double]
SQCloudVMBindDouble.restype = ctypes.c_bool


# Define the SQCloudVMBindInt signature
SQCloudVMBindInt = lib.SQCloudVMBindInt
SQCloudVMBindInt.argtypes = [ctypes.POINTER(SQCloudVM), ctypes.c_int, ctypes.c_int]
SQCloudVMBindInt.restype = ctypes.c_bool


# Define the SQCloudVMBindInt64 signature
SQCloudVMBindInt64 = lib.SQCloudVMBindInt64
SQCloudVMBindInt64.argtypes = [ctypes.POINTER(SQCloudVM), ctypes.c_int, ctypes.c_int64]
SQCloudVMBindInt64.restype = ctypes.c_bool


# Define the SQCloudVMBindNull signature
SQCloudVMBindNull = lib.SQCloudVMBindNull
SQCloudVMBindNull.argtypes = [ctypes.POINTER(SQCloudVM), ctypes.c_int]
SQCloudVMBindNull.restype = ctypes.c_bool


# Define the SQCloudVMBindText signature
SQCloudVMBindText = lib.SQCloudVMBindText
SQCloudVMBindText.argtypes = [ctypes.POINTER(SQCloudVM), ctypes.c_int, ctypes.c_char_p, ctypes.c_int32]
SQCloudVMBindText.restype = ctypes.c_bool


# Define the SQCloudVMBindBlob signature
SQCloudVMBindBlob = lib.SQCloudVMBindBlob
SQCloudVMBindBlob.argtypes = [ctypes.POINTER(SQCloudVM), ctypes.c_int, ctypes.c_void_p, ctypes.c_int32]
SQCloudVMBindBlob.restype = ctypes.c_bool


# Define the SQCloudVMBindZeroBlob signature
SQCloudVMBindZeroBlob = lib.SQCloudVMBindZeroBlob
SQCloudVMBindZeroBlob.argtypes = [ctypes.POINTER(SQCloudVM), ctypes.c_int, ctypes.c_int32]
SQCloudVMBindZeroBlob.restype = ctypes.c_bool


# Define the SQCloudVMColumnBlob signature
SQCloudVMColumnBlob = lib.SQCloudVMColumnBlob
SQCloudVMColumnBlob.argtypes = [ctypes.POINTER(SQCloudVM), ctypes.c_int, ctypes.POINTER(ctypes.c_uint32)]
SQCloudVMColumnBlob.restype = ctypes.c_void_p


# Define the SQCloudVMColumnText signature
SQCloudVMColumnText = lib.SQCloudVMColumnText
SQCloudVMColumnText.argtypes = [ctypes.POINTER(SQCloudVM), ctypes.c_int, ctypes.POINTER(ctypes.c_uint32)]
SQCloudVMColumnText.restype = ctypes.c_char_p


# Define the SQCloudVMColumnDouble signature
SQCloudVMColumnDouble = lib.SQCloudVMColumnDouble
SQCloudVMColumnDouble.argtypes = [ctypes.POINTER(SQCloudVM), ctypes.c_int]
SQCloudVMColumnDouble.restype = ctypes.c_double


# Define the SQCloudVMColumnInt32 signature
SQCloudVMColumnInt32 = lib.SQCloudVMColumnInt32
SQCloudVMColumnInt32.argtypes = [ctypes.POINTER(SQCloudVM), ctypes.c_int]
SQCloudVMColumnInt32.restype = ctypes.c_int32


# Define the SQCloudVMColumnInt64 signature
SQCloudVMColumnInt64 = lib.SQCloudVMColumnInt64
SQCloudVMColumnInt64.argtypes = [ctypes.POINTER(SQCloudVM), ctypes.c_int]
SQCloudVMColumnInt64.restype = ctypes.c_int64


# Define the SQCloudVMColumnLen signature
SQCloudVMColumnLen = lib.SQCloudVMColumnLen
SQCloudVMColumnLen.argtypes = [ctypes.POINTER(SQCloudVM), ctypes.c_int]
SQCloudVMColumnLen.restype = ctypes.c_int64


# Define the SQCloudVMColumnType signature
SQCloudVMColumnType = lib.SQCloudVMColumnType
SQCloudVMColumnType.argtypes = [ctypes.POINTER(SQCloudVM), ctypes.c_int]
SQCloudVMColumnType.restype = ctypes.c_void_p


# Define the SQCloudVMLastRowID signature
SQCloudVMLastRowID = lib.SQCloudVMLastRowID
SQCloudVMLastRowID.argtypes = [ctypes.POINTER(SQCloudVM)]
SQCloudVMLastRowID.restype = ctypes.c_int64


# Define the SQCloudVMChanges signature
SQCloudVMChanges = lib.SQCloudVMChanges
SQCloudVMChanges.argtypes = [ctypes.POINTER(SQCloudVM)]
SQCloudVMChanges.restype = ctypes.c_int64


# Define the SQCloudVMTotalChanges signature
SQCloudVMTotalChanges = lib.SQCloudVMTotalChanges
SQCloudVMTotalChanges.argtypes = [ctypes.POINTER(SQCloudVM)]
SQCloudVMTotalChanges.restype = ctypes.c_int64
