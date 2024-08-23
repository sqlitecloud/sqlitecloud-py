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
