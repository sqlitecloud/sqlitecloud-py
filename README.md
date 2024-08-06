# Driver for SQLite Cloud

<p align="center">
  <img src="https://sqlitecloud.io/social/logo.png" height="300" alt="SQLite Cloud logo">
</p>

![Build Status](https://github.com/sqlitecloud/sqlitecloud-py/actions/workflows/deploy.yaml/badge.svg "Build Status")
[![codecov](https://codecov.io/github/sqlitecloud/python/graph/badge.svg?token=38G6FGOWKP)](https://codecov.io/github/sqlitecloud/python)
![PyPI - Version](https://img.shields.io/pypi/v/sqlitecloud?link=https%3A%2F%2Fpypi.org%2Fproject%2FSqliteCloud%2F)
![PyPI - Downloads](https://img.shields.io/pypi/dm/sqlitecloud?link=https%3A%2F%2Fpypi.org%2Fproject%2FSqliteCloud%2F)
![PyPI - Python Version](https://img.shields.io/pypi/pyversions/sqlitecloud?link=https%3A%2F%2Fpypi.org%2Fproject%2FSqliteCloud%2F)


- [Driver for SQLite Cloud](#driver-for-sqlite-cloud)
- [Example](#example)
- [SQLite Cloud loves sqlite3](#sqlite-cloud-loves-sqlite3)
- [SQLite Cloud for Pandas DataFrame](#sqlite-cloud-for-pandas-dataframe)

---

[SQLite Cloud](https://sqlitecloud.io) is a powerful Python package that allows you to interact with the SQLite Cloud database seamlessly. It provides methods for various database operations. This package is designed to simplify database operations in Python applications, making it easier than ever to work with SQLite Cloud.


#### Compatibility with sqlite3 API

We aim for full compatibility with the Python built-in [sqlite3](https://docs.python.org/3.6/library/sqlite3.html) API (based on Python [PEP 249](https://peps.python.org/pep-0249)), with the primary distinction being that our driver connects to SQLite Cloud databases. This allows you to migrate your local SQLite databases to SQLite Cloud without needing to modify your existing Python code that uses the sqlite3 API.

- Documentation: Our API closely follows the sqlite3 API. You can refer to the sqlite3 documentation for most functionality. The list of implemented features are documented [here](https://github.com/sqlitecloud/sqlitecloud-py/issues/8).
- Source: [https://github.com/sqlitecloud/sqlitecloud-py](https://github.com/sqlitecloud/sqlitecloud-py)
- Site: [https://sqlitecloud.io](https://sqlitecloud.io/developers)

## Example

```bash
$ pip install sqlitecloud
```

```python
import sqlitecloud

# Open the connection to SQLite Cloud
conn = sqlitecloud.connect("sqlitecloud://myhost.sqlite.cloud:8860?apikey=myapikey")

# You can autoselect the database during the connect call
# by adding the database name as path of the SQLite Cloud
# connection string, eg:
# conn = sqlitecloud.connect("sqlitecloud://myhost.sqlite.cloud:8860/mydatabase?apikey=myapikey")
db_name = "chinook.sqlite"
conn.execute(f"USE DATABASE {db_name}")

cursor = conn.execute("SELECT * FROM albums WHERE AlbumId = ?", (1, ))
result = cursor.fetchone()

print(result)

conn.close()
```

## sqlitecloud loves sqlite3

Is your project based on the `sqlite3` library to interact with a SQLite database?

Just install `sqlitecloud` package from `pip` and change the module name! That's it!

Try it yourself:

```python
# import sqlitecloud
import sqlite3

# comment out the following line...
conn = sqlite3.connect(":memory:")

# ... and uncomment this line and import the sqlitecloud package
# (add the database name like in this connection string)
# conn = sqlitecloud.connect("sqlitecloud://myhost.sqlite.cloud:8860/mydatabase.sqlite?apikey=myapikey")

conn.execute("CREATE TABLE IF NOT EXISTS producers (ProducerId INTEGER PRIMARY KEY, name TEXT, year INTEGER)")
conn.executemany(
    "INSERT INTO producers (name, year) VALUES (?, ?)",
    [("Sony Music Entertainment", 2020), ("EMI Music Publishing", 2021)],
)

cursor = conn.execute("SELECT * FROM producers")

for row in cursor:
    print(row)
```

## SQLite Cloud for Pandas DataFrame

[Pandas](https://pypi.org/project/pandas/) is a Python package for data manipulation and analysis. It provides high-performance, easy-to-use data structures, such as DataFrame.

Use the connection to SQLite Cloud to:
- Insert data from a DataFrame into a SQLite Cloud database.
- Query SQLite Cloud and fetch the results into a DataFrame for further analysis.

Example:

```python
import io

import pandas as pd

import sqlitecloud

dfprices = pd.read_csv(
    io.StringIO(
        """DATE,CURRENCY,PRICE
    20230504,USD,201.23456
    20230503,USD,12.34567
    20230502,USD,23.45678
    20230501,USD,34.56789"""
    )
)

conn = sqlitecloud.connect("sqlitecloud://myhost.sqlite.cloud:8860/mydatabase.sqlite?apikey=myapikey")

conn.executemany("DROP TABLE IF EXISTS ?", [("PRICES",)])

# Write the dataframe to the SQLite Cloud database as a table PRICES
dfprices.to_sql("PRICES", conn, index=False)

# Create the dataframe from the table PRICES on the SQLite Cloud database
df_actual_prices = pd.read_sql("SELECT * FROM PRICES", conn)

# Inspect the dataframe
print(df_actual_prices.head())

# Perform a simple query on the dataframe
query_result = df_actual_prices.query("PRICE > 50.00")

print(query_result)
```
