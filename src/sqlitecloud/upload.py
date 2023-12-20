import ctypes
import os
from typing import Optional
from sqlitecloud.driver import CallbackFunc, SQCloudConnect, SQCloudUploadDatabase


def xCallback(xdata, buffer, blen, ntot, nprogress):  # pylint: disable=W0613
    nread = os.read(xdata, blen.contents.value)
    if nread == -1:
        return -1
    if nread == 0:
        print("UPLOAD COMPLETE\n\n")
    else:
        print(f"{(nprogress + len(nread)) / ntot * 100:.2f}%")

    blen.contents.value = len(nread)
    return 0


def upload_db(
    connection: SQCloudConnect, dbname: str, key: Optional[str], filename: str
) -> None:
    fd_value = os.open(filename, os.O_RDONLY)
    fd_void_ptr = ctypes.c_void_p(fd_value)
    dbsize = os.path.getsize(filename)
    print("dbsize", dbsize)
    key_val = key.encode() if key else None
    success = SQCloudUploadDatabase(
        connection,
        dbname.encode(),
        key_val,
        fd_void_ptr,
        dbsize,
        CallbackFunc(xCallback),
    )
    print("upload_db", success)
