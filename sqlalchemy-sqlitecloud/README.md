# SQLite Cloud dialect for SQLAlchermy (beta)

This package makes SQLAlchemy to work with SQLite Cloud.

The dialect is based on the `sqlite` dialect included in SQLAlchemy.

## Beta version

This dialect is an early version and it works on Python >= 3.6.

**Tested only on `Python 3.6` and `SQLAlchemy 1.4`.**

The dialect as been tested against the SQLAlchemy `test_suite` as reported in the [documentation](https://github.com/sqlalchemy/sqlalchemy/blob/rel_1_4_53/README.dialects.rst).

You can follow the status of the tests that are still failing [in the dedicated issue](https://github.com/sqlitecloud/sqlitecloud-py/issues/21#issuecomment-2305162632).

References:
- [Official SQLAlchemy documentation](https://docs.sqlalchemy.org/en/14/index.html)
- [https://www.sqlalchemy.org/](https://www.sqlalchemy.org/)


# Install and Usage

```bash
$ pip install sqlalchemy-sqlitecloud
```

> Get your SQLite Cloud connection string from the SQLite Cloud dashboard or register on [sqlitecloud.io](https://sqlitecloud.io) to get one.

Create the SQLAlchemy engine by setting the SQLite Cloud connection string. Add the prefix `sqlite+` like:

```python
from sqlalchemy import create_engine

engine = create_engine('sqlite+sqlitecloud://mynode.sqlite.io?apikey=key1234')
```


# Run the Test Suite

Install the `sqlitecloud` package with:

```bash
$ pip install sqlitecloud # last version
```
or install the reference to the local version:


```bash
$ cd ../src # sqlitecloud src directory
$ pip install -e .
```

Run the test suite with:

```bash
$ cd sqlalchemy-sqlitecloud
$ pytest
```

> Note:  VSCode Test Explorer and VSCode GUI debugger doesn't work because the actual implementation
is not in the `test/test_suite.py` file. It cannot find the tests source code in the third-parties directory.

Use `pytest --pdb` with `pdb.set_trace()` to debug with command line:

 - `s` step into
 - `n` next line
 - `r` jump to the end of the function
 - `c` continue
 - `a` print all variables
