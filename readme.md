# Python SDK for SqliteCloud



![Build Status](https://github.com/codermine/sqlitecloud-python-sdk/actions/workflows/deploy.yaml/badge.svg "Build Status") ![Jupyter Notebook](https://img.shields.io/badge/jupyter-%23FA0F00.svg?style=for-the-badge&logo=jupyter&logoColor=white)

# Usage

## Import SqliteCloudClient and SqliteCloudAccount

SqliteCloudAccount is the class rapresenting your auth data for SqliteCloud

SqliteCloudClient is the class managing the connection for you


```python
from sqlitecloud.conn_info import user,password,host,db_name,port
from sqlitecloud.client import SqliteCloudClient, SqliteCloudAccount
```

    Loading SQLITECLOUD lib from: /Users/sam/projects/codermine/sqlitecloud-sdk/C/libsqcloud.so


## Init a connection

Initialize the client with account connection info


```python
account = SqliteCloudAccount(user, password, host, db_name, port)
client = SqliteCloudClient(cloud_account=account)
conn = client.open_connection()
query = "select * from employees;"
result = client.exec_query(query, conn)
```

    select * from employees;


The result is an iterable


```python
for r in result:
    print(r)
```


Then you are done clean up the connection


```python
client.disconnect(conn)

```

You can bind values to parametric queries: you can pass parameters as positional values in an array


```python
new_connection = client.open_connection()
result = client.exec_statement("select * from employees where emp_id = ?", [1],conn=new_connection)
for r in result:
    print(r)
client.disconnect(conn)
```

