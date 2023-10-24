import os

user = os.environ.get("SQLITE_USER", "ADMIN")
password = os.environ.get("SQLITE_PASSWORD", "F77VNEnVTS")
host = os.environ.get("SQLITE_HOST", "qrznfgtzm.sqlite.cloud")
db_name = os.environ.get("SQLITE_DB", "people")
port = os.environ.get("SQLITE_PORT", 8860)
