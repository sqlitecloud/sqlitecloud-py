from typing import Any
from sqlitecloud.client import SqliteCloudAccount, SqliteCloudClient
from sqlitecloud.conn_info import user, password, host, db_name, port
from sqlitecloud.driver import SQCloudConnect
from sqlitecloud.pubsub import SQCloudPubSubCallback
from sqlitecloud.resultset import SqliteCloudResultSet
from sqlitecloud.wrapper_types import SQCloudResult


def _l_cb(conn: SQCloudConnect, res: SQCloudResult, data: Any = None):
    print("\t start callback")



def test_sqlite_cloud_client_exec_query():
    print("running callbacks")
    cb: SQCloudPubSubCallback = _l_cb
    account = SqliteCloudAccount(user, password, host, db_name, port)
    client = SqliteCloudClient(cloud_account=account, pub_subs=[cb])
    conn = client.open_connection()
    query = 'NOTIFY channel1 "Hello World"'
    # query = "select * from employees;"
    client.exec_query(query, conn)
    #client.disconnect(conn)
