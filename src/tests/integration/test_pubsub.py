from time import sleep
import time

import pytest

from sqlitecloud.pubsub import SqliteCloudPubSub
from sqlitecloud.resultset import SqliteCloudResultSet
from sqlitecloud.types import (
    SQCLOUD_ERRCODE,
    SQCLOUD_PUBSUB_SUBJECT,
    SQCLOUD_RESULT_TYPE,
    SQCloudException,
)


class TestPubSub:
    def test_listen_channel_and_notify(self, sqlitecloud_connection):
        connection, _ = sqlitecloud_connection

        callback_called = False

        def assert_callback(conn, result, data):
            nonlocal callback_called

            if isinstance(result, SqliteCloudResultSet):
                assert result.tag == SQCLOUD_RESULT_TYPE.RESULT_JSON
                assert data == ["somedata"]
                callback_called = True

        pubsub = SqliteCloudPubSub()
        type = SQCLOUD_PUBSUB_SUBJECT.CHANNEL
        channel = "channel" + str(int(time.time()))

        pubsub.create_channel(connection, channel)
        pubsub.listen(connection, type, channel, assert_callback, ["somedata"])

        pubsub.notify_channel(connection, channel, "somedata2")

        # wait for callback to be called
        sleep(1)

        assert callback_called

    def test_unlisten_channel(self, sqlitecloud_connection):
        connection, _ = sqlitecloud_connection

        pubsub = SqliteCloudPubSub()
        type = SQCLOUD_PUBSUB_SUBJECT.CHANNEL
        channel_name = "channel" + str(int(time.time()))

        pubsub.create_channel(connection, channel_name)
        pubsub.listen(connection, type, channel_name, lambda conn, result, data: None)

        result = pubsub.list_connections(connection)
        assert channel_name in result.data

        pubsub.unlisten(connection, type, channel_name)

        result = pubsub.list_connections(connection)

        assert channel_name not in result.data
        assert connection.pubsub_callback is None
        assert connection.pubsub_data is None

    def test_create_channel_to_fail_if_exists(self, sqlitecloud_connection):
        connection, _ = sqlitecloud_connection

        pubsub = SqliteCloudPubSub()
        channel_name = "channel" + str(int(time.time()))

        pubsub.create_channel(connection, channel_name, if_not_exists=True)

        with pytest.raises(SQCloudException) as e:
            pubsub.create_channel(connection, channel_name, if_not_exists=False)

        assert (
            e.value.errmsg
            == f"Cannot create channel {channel_name} because it already exists."
        )
        assert e.value.errcode == SQCLOUD_ERRCODE.GENERIC.value

    def test_is_connected(self, sqlitecloud_connection):
        connection, _ = sqlitecloud_connection

        pubsub = SqliteCloudPubSub()
        channel_name = "channel" + str(int(time.time()))

        assert not pubsub.is_connected(connection)

        pubsub.create_channel(connection, channel_name, if_not_exists=True)
        pubsub.listen(connection, SQCLOUD_PUBSUB_SUBJECT.CHANNEL, channel_name, lambda conn, result, data: None)

        assert pubsub.is_connected(connection)

    def test_set_pubsub_only(self, sqlitecloud_connection):
        connection, client = sqlitecloud_connection

        callback_called = False

        def assert_callback(conn, result, data):
            nonlocal callback_called

            if isinstance(result, SqliteCloudResultSet):
                assert result.get_result() is not None
                callback_called = True

        pubsub = SqliteCloudPubSub()
        type = SQCLOUD_PUBSUB_SUBJECT.CHANNEL
        channel = "channel" + str(int(time.time()))

        pubsub.create_channel(connection, channel, if_not_exists=True)
        pubsub.listen(connection, type, channel, assert_callback)

        pubsub.set_pubsub_only(connection)

        assert not client.is_connected(connection)
        assert pubsub.is_connected(connection)

        connection2 = client.open_connection()
        pubsub2 = SqliteCloudPubSub()
        pubsub2.notify_channel(connection2, channel, "message-in-a-bottle")

        # wait for callback to be called
        sleep(2)

        assert callback_called

        client.disconnect(connection2)

    def test_listen_table_for_update(self, sqlitecloud_connection):
        connection, client = sqlitecloud_connection

        callback_called = False

        def assert_callback(conn, result, data):
            nonlocal callback_called

            if isinstance(result, SqliteCloudResultSet):
                assert result.tag == SQCLOUD_RESULT_TYPE.RESULT_JSON
                assert new_name in result.get_result()
                assert data == ["somedata"]
                callback_called = True

        pubsub = SqliteCloudPubSub()
        type = SQCLOUD_PUBSUB_SUBJECT.TABLE
        new_name = "Rock"+ str(int(time.time()))

        pubsub.listen(connection, type, "genres", assert_callback, ["somedata"])

        client.exec_query(f"UPDATE genres SET Name = '{new_name}' WHERE GenreId = 1;", connection)

        # wait for callback to be called
        sleep(1)

        assert callback_called