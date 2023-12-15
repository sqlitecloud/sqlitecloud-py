# Python SDK for SqliteCloud

![Build Status](https://github.com/codermine/sqlitecloud-python-sdk/actions/workflows/deploy.yaml/badge.svg "Build Status") ![Jupyter Notebook](https://img.shields.io/badge/jupyter-%23FA0F00.svg?style=plastic&logo=jupyter&logoColor=white)

![SQLiteCloud Logo](https://sqlitecloud.io/static/image/c19460c9ed65bc09aea9.png)
# Usage

## Import SqliteCloudClient and SqliteCloudAccount

SqliteCloudAccount is the class rapresenting your auth data for SqliteCloud

SqliteCloudClient is the class managing the connection for you


```python
from sqlitecloud.client import SqliteCloudClient, SqliteCloudAccount
```

## Init a connection

Initialize the client with account connection info

```python
account = SqliteCloudAccount(user, password, host, db_name, port)
client = SqliteCloudClient(cloud_account=account)
conn = client.open_connection()
query = "SELECT * FROM table_name;"
result = client.exec_query(query, conn)
```


The result is an iterable


```python
for row in result:
    print(row)
```


Then you are done clean up the connection


```python
client.disconnect(conn)

```

You can bind values to parametric queries: you can pass parameters as positional values in an array


```python
new_connection = client.open_connection()
result = client.exec_statement("SELECT * FROM table_name WHERE id = ?", [1],conn=new_connection)
for row in result:
    print(row)
client.disconnect(conn)
```

