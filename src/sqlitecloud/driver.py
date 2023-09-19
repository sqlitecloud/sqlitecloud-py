import os
import ctypes

from sqlitecloud.wrapper_types import SQCloudConfig, SQCloudResult

lib = ctypes.CDLL(os.getenv("SQLITECLOUD_DRIVER_PATH", "libsqcloud.so"))
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
