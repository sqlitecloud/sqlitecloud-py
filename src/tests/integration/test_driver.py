import tempfile

from sqlitecloud.driver import Driver


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
