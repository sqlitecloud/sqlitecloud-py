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
