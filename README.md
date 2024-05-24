# Python SDK for SQLite Cloud

<p align="center">
  <img src="https://sqlitecloud.io/social/logo.png" height="300" alt="SQLite Cloud logo">
</p>

![Build Status](https://github.com/sqlitecloud/python/actions/workflows/deploy.yaml/badge.svg "Build Status")
[![codecov](https://codecov.io/github/sqlitecloud/python/graph/badge.svg?token=38G6FGOWKP)](https://codecov.io/github/sqlitecloud/python)
![PyPI - Version](https://img.shields.io/pypi/v/sqlitecloud?link=https%3A%2F%2Fpypi.org%2Fproject%2FSqliteCloud%2F)
![PyPI - Downloads](https://img.shields.io/pypi/dm/sqlitecloud?link=https%3A%2F%2Fpypi.org%2Fproject%2FSqliteCloud%2F)
![PyPI - Python Version](https://img.shields.io/pypi/pyversions/sqlitecloud?link=https%3A%2F%2Fpypi.org%2Fproject%2FSqliteCloud%2F)


[SQLiteCloud](https://sqlitecloud.io) is a powerful Python package that allows you to interact with the SQLite Cloud backend server seamlessly. It provides methods for various database operations. This package is designed to simplify database operations in Python applications, making it easier than ever to work with SQLite Cloud.

- Site: [https://sqlitecloud.io](https://sqlitecloud.io/developers)
- Documentation: https://..._coming!_
- Source: [https://github.com/sqlitecloud/python](https://github.com/sqlitecloud/python)

## Installation

You can install SqliteCloud Package using Python Package Index (PYPI):

```bash
$ pip install sqlitecloud
```

## Usage
<hr>

```python
from sqlitecloud.client import SqliteCloudClient
from sqlitecloud.types import SqliteCloudAccount
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
account = SqliteCloudAccount("sqlitecloud://user:pass@host.com:port/dbname?apikey=myapikey")
client = SqliteCloudClient(cloud_account=account)
conn = client.open_connection()
```

### _Execute a query_
You can bind values to parametric queries: you can pass parameters as positional values in an array
```python
result = client.exec_query(
    "SELECT * FROM table_name WHERE id = 1"
    conn=conn
)
```

### _Iterate result_
result is an iterable object
```python
for row in result:
    print(row)
```

### _Specific value_
```python
result.get_value(0, 0)
```

### _Column name_
```python
result.get_name(0)
```

### _Close connection_

```python
client.disconnect(conn)
```
