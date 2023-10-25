from typing import Any
from experiments.test_ctypes import SQCloudConnect
from sqlitecloud.client import SqliteCloudAccount, SqliteCloudClient
from sqlitecloud.conn_info import user,password,host,db_name,port
from sqlitecloud.pubsub import SQCloudPubSubCallback
from sqlitecloud.wrapper_types import SQCloudResult

def test_sqlite_cloud_client_exec_query():
    print("running callbacks")
    account = SqliteCloudAccount(user, password, host, db_name, port)
    client = SqliteCloudClient(cloud_account=account)
    def _l_cb(conn:SQCloudConnect,res:SQCloudResult,data:Any):
        print("++++++++++++++++++++++++ cb engaged +++++++++++",str(data))

    cb: SQCloudPubSubCallback = _l_cb
    conn = client.open_connection(pub_sub_callback=cb)
    query = "select * from employees;"
    result = client.exec_query(query, conn)
    first_element = next(result)
    assert len(first_element)  > 0
    client.disconnect(conn)
