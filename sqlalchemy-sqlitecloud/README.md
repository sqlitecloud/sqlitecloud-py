# SQLite Cloud Dialect for SQLAlchemy (Beta)

This package enables SQLAlchemy to work seamlessly with SQLite Cloud. The dialect is built upon the existing `sqlite` dialect in SQLAlchemy.

## Beta Version

This dialect is in its early stages and is compatible with Python >= 3.6.

**Note:** It has been tested only `SQLAlchemy 1.4`.

The dialect has undergone testing against the SQLAlchemy `test_suite`, as outlined in the [official documentation](https://github.com/sqlalchemy/sqlalchemy/blob/rel_1_4_53/README.dialects.rst).

You can track the progress of the remaining test issues [in this issue](https://github.com/sqlitecloud/sqlitecloud-py/issues/21#issuecomment-2305162632).

_The same tests failed and passed on both Python 3.6 and Python 3.11._

## References

- [SQLAlchemy Documentation](https://docs.sqlalchemy.org/en/14/index.html)
- [SQLAlchemy Official Website](https://www.sqlalchemy.org/)

## Installation and Usage

To install the package, use the following command:

```bash
$ pip install sqlalchemy-sqlitecloud
```

> Get your SQLite Cloud connection string from the SQLite Cloud dashboard, or register at [sqlitecloud.io](https://sqlitecloud.io) to get one.

Create an SQLAlchemy engine using the SQLite Cloud connection string::

```python
from sqlalchemy import create_engine

engine = create_engine('sqlitecloud://mynode.sqlite.io?apikey=key1234')
```

### Example

_The example is based on `chinook.sqlite` database on SQLite Cloud_

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



# Run the Test Suite

To run the test suite, first install the sqlitecloud package:

```bash
$ pip install sqlitecloud # last version
```
or install the reference to the local version:


```bash
$ cd ../src # sqlitecloud src directory
$ pip install -e .
```

Then, run the test suite with:

```bash
$ cd sqlalchemy-sqlitecloud
$ pytest
```

> Note:  VSCode Test Explorer and VSCode GUI debugger doesn't work because the actual implementation of tests
is not in the `test/test_suite.py` file. The test source code is located in a third-party directory and it's not recognized.

For command-line debugging, use `pytest --pdb` with `pdb.set_trace()`.

Some useful `pdb` commands include:

 - `s` step into
 - `n` next line
 - `r` jump to the end of the function
 - `c` continue
 - `w` print stack trace

 More info: [https://docs.python.org/3/library/pdb.html](https://docs.python.org/3/library/pdb.html)
