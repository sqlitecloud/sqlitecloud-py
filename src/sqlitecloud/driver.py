import os
import ctypes

from sqlitecloud.wrapper_types import SQCloudConfig

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
