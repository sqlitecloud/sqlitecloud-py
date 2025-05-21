import hashlib
import random
import tempfile
import uuid

from sqlitecloud.driver import Driver
from sqlitecloud.resultset import SQLiteCloudOperationResult, SQLiteCloudResult


class TestDriver:
    def test_download_missing_database_without_error_when_expected(
        self, sqlitecloud_connection
    ):
        driver = Driver()

        connection, _ = sqlitecloud_connection

        temp_file = tempfile.mkstemp(prefix="missing")[1]

        if_exists = True

        with open(temp_file, "wb") as fd:
            driver.download_database(
                connection,
                "missing.sqlite",
                fd,
                lambda x, y, z, k: None,
                if_exists=if_exists,
            )

    def test_prepare_statement_select(self, sqlitecloud_connection):
        driver = Driver()

        connection, _ = sqlitecloud_connection

        query = "SELECT AlbumId FROM Albums WHERE AlbumId = ?"
        bindings = [1]

        result: SQLiteCloudResult = driver.execute_statement(
            query, bindings, connection
        )

        assert result.nrows == 1
        assert result.ncols == 1
        assert result.get_name(0) == "AlbumId"
        assert result.get_value(0, 0) == 1

    def test_prepare_statement_insert(self, sqlitecloud_connection):
        driver = Driver()

        connection, _ = sqlitecloud_connection

        name = "MyGenre" + str(uuid.uuid4())
        query = "INSERT INTO genres (Name) VALUES (?)"
        bindings = [name]

        result = driver.execute_statement(query, bindings, connection)

        result_select = driver.execute_statement(
            "SELECT GenreId FROM genres WHERE Name = ?", (name,), connection
        )

        assert isinstance(result, SQLiteCloudOperationResult)
        assert result.type == 10
        assert result.index == 0
        assert result.rowid == result_select.get_value(0, 0)
        assert result.changes == 1
        assert result.total_changes == 1
        assert result.finalized == 1

    def test_prepare_statement_delete(self, sqlitecloud_connection):
        driver = Driver()

        connection, _ = sqlitecloud_connection

        name = "MyGenre" + str(random.randint(0, 1000))
        result_insert = driver.execute_statement(
            "INSERT INTO genres (Name) VALUES (?)", [name], connection
        )

        result = driver.execute_statement(
            "DELETE FROM genres WHERE Name = ?", (name,), connection
        )

        assert isinstance(result, SQLiteCloudOperationResult)
        assert result.type == 10
        assert result.index == 0
        assert result.rowid == result_insert.rowid
        assert result.changes == 1
        assert result.total_changes == 2  # insert + delete
        assert result.finalized == 1

    def test_prepare_statement_update(self, sqlitecloud_connection):
        driver = Driver()

        connection, _ = sqlitecloud_connection

        name = "MyGenre" + str(random.randint(0, 1000))
        result_insert = driver.execute_statement(
            "INSERT INTO genres (Name) VALUES (?)", [name], connection
        )

        new_name = "AnotherMyGenre" + str(random.randint(0, 1000))
        result = driver.execute_statement(
            "UPDATE genres SET Name = ? WHERE GenreId = ?",
            (new_name, result_insert.rowid),
            connection,
        )

        assert isinstance(result, SQLiteCloudOperationResult)
        assert result.type == 10
        assert result.index == 0
        assert result.rowid == result_insert.rowid
        assert result.changes == 1
        assert result.total_changes == 2  # insert + update
        assert result.finalized == 1

    def test_prepare_statement_insert_blob(self, sqlitecloud_connection):
        driver = Driver()

        connection, _ = sqlitecloud_connection

        hash_data = hashlib.sha256(b"my blob data").digest()

        driver.execute_statement(
            "CREATE TABLE IF NOT EXISTS blobs (id INTEGER PRIMARY KEY, hash BLOB NOT NULL);",
            [],
            connection,
        )
        insert_result = driver.execute_statement(
            "INSERT INTO blobs (hash) VALUES (?);", (hash_data,), connection
        )
        result = driver.execute_statement(
            "SELECT id, hash FROM blobs WHERE id = ? and hash = ?;",
            (insert_result.rowid, hash_data),
            connection,
        )

        assert result.nrows == 1
        assert result.get_value(0, 1) == hash_data
        assert result.get_value(0, 0) == insert_result.rowid
