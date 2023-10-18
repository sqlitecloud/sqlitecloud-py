import ctypes
from ctypes import POINTER, Structure, c_int, py_object, pythonapi
import os
import sys
from typing import Optional
from sqlitecloud.driver import CallbackFunc, SQCloudConnect, SQCloudUploadDatabase

# Define the callback function
def xCallback(xdata, buffer, blen, ntot, nprogress):
    fd = ctypes.cast(xdata, ctypes.POINTER(ctypes.c_int)).contents.value
    """  nread = os.read(fd, buffer, blen.contents.value)
    if nread == -1:
        return -1
    elif nread == 0:
        print("UPLOAD COMPLETE\n\n")
    else:
        print(f"{(nprogress.value + nread) / ntot * 100:.2f}%")

    blen.contents.value = nread """
    return 0



def upload_db( connection: SQCloudConnect,dbname:str,key:Optional[str], filename:str)->None:

        fd_value = os.open("test.db", os.O_RDONLY)
        fd_void_ptr = ctypes.c_void_p(fd_value)
        dbsize  = os.path.getsize(filename)
        print("dbsize",dbsize)
        key_val = key.encode() if key else None
        success = SQCloudUploadDatabase(connection, dbname.encode(),key_val, fd_void_ptr, dbsize, CallbackFunc(xCallback))
        print("upload_db",success)
