# Python SDK for SqliteCloud

![Build Status](https://github.com/sqlitecloud/python/actions/workflows/deploy.yaml/badge.svg "Build Status") ![Jupyter Notebook](https://img.shields.io/badge/jupyter-%23FA0F00.svg?style=plastic&logo=jupyter&logoColor=white)

SQLiteCloud is a powerful Python package that allows you to interact with the SQLite Cloud backend server seamlessly. It provides methods for various database operations. This package is designed to simplify database operations in Python applications, making it easier than ever to work with SQLite Cloud.


## Installation

You can install SqliteCloud Package using Python Package Index (PYPI):

```bash
$ pip install SqliteCloud
```

- Follow the instructions reported here https://github.com/sqlitecloud/sdk/tree/master/C to build the driver.

- Set SQLITECLOUD_DRIVER_PATH environment variable to the path of the driver file build.

## Usage
<hr>

```python
from sqlitecloud.client import SqliteCloudClient, SqliteCloudAccount
```

### _Init a connection_

#### Using explicit configuration

```python
account = SqliteCloudAccount(user, password, host, db_name, port)
client = SqliteCloudClient(cloud_account=account)
conn = client.open_connection()
```

#### _Using string configuration_

```python
account = SqliteCloudAccount("sqlitecloud://user:pass@host.com:port/dbname?timeout=10&key2=value2&key3=value3")
client = SqliteCloudClient(cloud_account=account)
conn = client.open_connection()
```

### _Execute a query_
You can bind values to parametric queries: you can pass parameters as positional values in an array
```python
result = client.exec_statement(
    "SELECT * FROM table_name WHERE id = ?",
    [1],
    conn=conn
)
```

### _Iterate result_
result is an iterable object
```python
for row in result:
    print(row)
```

### _Close connection_

```python 
client.disconnect(conn)
```
