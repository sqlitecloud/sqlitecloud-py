import os
import uuid
import pytest
from sqlitecloud.client import SqliteCloudClient
from sqlitecloud.types import SQCloudConnect, SqliteCloudAccount
from sqlitecloud.upload import upload_db

class TestUpload:

    @pytest.fixture()
    def sqlitecloud_connection(self):
        account = SqliteCloudAccount()
        account.username = os.getenv("SQLITE_USER")
        account.password = os.getenv("SQLITE_PASSWORD")
        account.dbname = os.getenv("SQLITE_DB")
        account.hostname = os.getenv("SQLITE_HOST")
        account.port = 8860

        client = SqliteCloudClient(cloud_account=account)

        connection = client.open_connection()
        assert isinstance(connection, SQCloudConnect)

        yield (connection, client)

        client.disconnect(connection)
        
    def test_upload_db(self, sqlitecloud_connection):
        connection, client = sqlitecloud_connection

        dbname = f"testUploadDb{str(uuid.uuid4())}"
        key = None
        filename = os.path.join(os.path.dirname(__file__), '..', 'assets', 'test.db')

        result = upload_db(connection, dbname, key, filename)

        assert result == True

        try:
            rowset = client.exec_query(f"USE DATABASE {dbname}; SELECT * FROM contacts", connection)

            assert rowset.nrows == 1
            assert rowset.ncols == 5
            assert rowset.get_value(0, 1) == 'John'
            assert rowset.get_name(4) == 'phone'
        finally:
            # delete uploaded database
            client.exec_query(f"UNUSE DATABASE; REMOVE DATABASE {dbname}", connection)
