# Driver for SQLite Cloud

<p align="center">
  <img src="https://sqlitecloud.io/social/logo.png" height="300" alt="SQLite Cloud logo">
</p>

![Build Status](https://github.com/sqlitecloud/sqlitecloud-py/actions/workflows/test.yaml/badge.svg "Build Status")
[![codecov](https://codecov.io/gh/sqlitecloud/sqlitecloud-py/graph/badge.svg?token=38G6FGOWKP)](https://codecov.io/gh/sqlitecloud/sqlitecloud-py)
![PyPI - Version](https://img.shields.io/pypi/v/sqlitecloud?link=https%3A%2F%2Fpypi.org%2Fproject%2FSqliteCloud%2F)
![PyPI - Downloads](https://img.shields.io/pypi/dm/sqlitecloud?link=https%3A%2F%2Fpypi.org%2Fproject%2FSqliteCloud%2F)
![PyPI - Python Version](https://img.shields.io/pypi/pyversions/sqlitecloud?link=https%3A%2F%2Fpypi.org%2Fproject%2FSqliteCloud%2F)


- [SQLite Cloud](#)
- [Compatibility with sqlite3 API](#compatibility-with-sqlite3-api)
  - [Autocommit transactions: Difference between sqlitecloud and sqlite3](#autocommit-transactions-difference-between-sqlitecloud-and-sqlite3)
- [Installation and Usage](#installation-and-usage)
- [SQLite Cloud loves sqlite3](#sqlite-cloud-loves-sqlite3)
- [SQLite Cloud for SQLAlchemy (beta)](#sqlite-cloud-for-sqlalchemy-beta)
- [SQLite Cloud for Pandas DataFrame](#sqlite-cloud-for-pandas-dataframe)

---

[SQLite Cloud](https://sqlitecloud.io) is a powerful Python package that allows you to interact with the SQLite Cloud database seamlessly. It provides methods for various database operations. This package is designed to simplify database operations in Python applications, making it easier than ever to work with SQLite Cloud.


## Compatibility with sqlite3 API

We aim for full compatibility with the Python built-in [sqlite3](https://docs.python.org/3.6/library/sqlite3.html) API (based on Python DBAPI 2.0 [PEP 249](https://peps.python.org/pep-0249)), with the primary distinction being that our driver connects to SQLite Cloud databases. This allows you to migrate your local SQLite databases to SQLite Cloud without needing to modify your existing Python code that uses the sqlite3 API.

- Documentation: Our API closely follows the sqlite3 API. You can refer to the sqlite3 documentation for most functionality. The list of implemented features are documented [here](https://github.com/sqlitecloud/sqlitecloud-py/issues/8).
- Source: [https://github.com/sqlitecloud/sqlitecloud-py](https://github.com/sqlitecloud/sqlitecloud-py)
- Site: [https://sqlitecloud.io](https://sqlitecloud.io/developers)

### Autocommit transactions: Difference between sqlitecloud and sqlite3

In `sqlitecloud`, autocommit is **always enabled**, and we currently do not support disabling it. This means that the `isolation_level` is always set to `None`, resulting in autocommit being permanently on.

This behavior differs from the sqlite3 Python module, where autocommit can be controlled (see details in the section [Controlling Transactions](https://docs.python.org/3.6/library/sqlite3.html#controlling-transactions) in the official documentation).

To manage transactions in sqlitecloud, you should explicitly use the `BEGIN`, `ROLLBACK`, `SAVEPOINT`, and `RELEASE` commands as needed.

## Installation and Usage

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

## SQLite Cloud for SQLAlchemy (beta)

_This is an initial release, features and stability may not be guaranteed in all scenarios._

_If you encounter any bugs or issues, please feel free to open an issue on our GitHub repository._

We’ve implemented the initial support for `sqlitecloud` with [SQLAlchemy](https://www.sqlalchemy.org/), allowing you to utilize all standard SQLAlchemy operations and queries.
For further information, please see the dedicated [REDAME](https://github.com/sqlitecloud/sqlitecloud-py/tree/main/sqlalchemy-sqlitecloud).

### Example

_The example is based on `chinook.sqlite` databse on SQLite Cloud_

Install the package:

```bash
$ pip install sqlalchemy-sqlitecloud
```


```python
import sqlalchemy
from sqlalchemy import Column, ForeignKey, Integer, String
from sqlalchemy.dialects import registry
from sqlalchemy.orm import backref, declarative_base, relationship, sessionmaker

Base = declarative_base()


class Artist(Base):
    __tablename__ = "artists"

    ArtistId = Column("ArtistId", Integer, primary_key=True)
    Name = Column("Name", String)
    Albums = relationship("Album", backref=backref("artist"))


class Album(Base):
    __tablename__ = "albums"

    AlbumId = Column("AlbumId", Integer, primary_key=True)
    ArtistId = Column("ArtistId", Integer, ForeignKey("artists.ArtistId"))
    Title = Column("Title", String)

# SQLite Cloud connection string
connection_string = "sqlitecloud://myhost.sqlite.cloud:8860/mydatabase.sqlite?apikey=myapikey"

engine = sqlalchemy.create_engine(connection_string)
Session = sessionmaker(bind=engine)
session = Session()

name = "John Doe"
query = sqlalchemy.insert(Artist).values(Name=name)
result_insert = session.execute(query)

title = "The Album"
query = sqlalchemy.insert(Album).values(
    ArtistId=result_insert.lastrowid, Title=title
)
session.execute(query)

query = (
    sqlalchemy.select(Artist, Album)
    .join(Album, Artist.ArtistId == Album.ArtistId)
    .where(Artist.ArtistId == result_insert.lastrowid)
)

result = session.execute(query).fetchone()

print("Artist Name: " + result[0].Name)
print("Album Title: " + result[1].Title)

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
