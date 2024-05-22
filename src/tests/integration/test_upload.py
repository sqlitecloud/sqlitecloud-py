import os
import uuid

from sqlitecloud.upload import upload_db


class TestUpload:
    def test_upload_db(self, sqlitecloud_connection):
        connection, client = sqlitecloud_connection

        dbname = f"testUploadDb{str(uuid.uuid4())}"
        key = None
        filename = os.path.join(os.path.dirname(__file__), "..", "assets", "test.db")

        upload_db(connection, dbname, key, filename)

        try:
            rowset = client.exec_query(
                f"USE DATABASE {dbname}; SELECT * FROM contacts", connection
            )

            assert rowset.nrows == 1
            assert rowset.ncols == 5
            assert rowset.get_value(0, 1) == "John"
            assert rowset.get_name(4) == "phone"
        finally:
            # delete uploaded database
            client.exec_query(f"UNUSE DATABASE; REMOVE DATABASE {dbname}", connection)
