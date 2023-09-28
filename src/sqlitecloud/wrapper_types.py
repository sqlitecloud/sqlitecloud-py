import ctypes


class SQCloudConfig(ctypes.Structure):
    _fields_ = [
        ("username", ctypes.c_char_p),
        ("password", ctypes.c_char_p),
        ("database", ctypes.c_char_p),
        ("timeout", ctypes.c_int),
        ("family", ctypes.c_int),
        ("compression", ctypes.c_bool),
        ("sqlite_mode", ctypes.c_bool),
        ("zero_text", ctypes.c_bool),
        ("password_hashed", ctypes.c_bool),
        ("nonlinearizable", ctypes.c_bool),
        ("db_memory", ctypes.c_bool),
        ("no_blob", ctypes.c_bool),
        ("db_create", ctypes.c_bool),
        ("max_data", ctypes.c_int),
        ("max_rows", ctypes.c_int),
        ("max_rowset", ctypes.c_int),
        ("tls_root_certificate", ctypes.c_char_p),
        ("tls_certificate", ctypes.c_char_p),
        ("tls_certificate_key", ctypes.c_char_p),
        ("insecure", ctypes.c_bool),
        ("callback", ctypes.c_void_p),  # This assumes config_cb is of type void pointer
        ("data", ctypes.c_void_p),
    ]


class SQCloudResult(ctypes.Structure):
    _fields_ = [
        ("num_rows", ctypes.c_int),
        ("num_columns", ctypes.c_int),
        ("column_names", ctypes.POINTER(ctypes.c_char_p)),
        ("column_types", ctypes.POINTER(ctypes.c_int)),
        ("data", ctypes.POINTER(ctypes.POINTER(ctypes.c_char))),
    ]


class SQCLOUD_VALUE_TYPE(ctypes.c_uint):
    VALUE_INTEGER = 1
    VALUE_FLOAT = 2
    VALUE_TEXT = 3
    VALUE_BLOB = 4
    VALUE_NULL = 5


class SQCLOUD_RESULT_TYPE(ctypes.c_uint):
    RESULT_OK = 0
    RESULT_FLOAT = 2
    RESULT_STRING = 2
    RESULT_INTEGER = 3
    RESULT_ERROR = 4
    RESULT_ROWSET = 5
    RESULT_ARRAY = 6
    RESULT_NULL = 7
    RESULT_JSON = 8
    RESULT_BLOB = 9
