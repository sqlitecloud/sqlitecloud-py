import json
import logging
import select
import socket
import ssl
import threading
from io import BufferedReader, BufferedWriter
from typing import Any, Callable, List, Optional, Tuple, Union

import lz4.block

from sqlitecloud.datatypes import (
    SQLITECLOUD_CMD,
    SQLITECLOUD_DEFAULT,
    SQLITECLOUD_INTERNAL_ERRCODE,
    SQLITECLOUD_ROWSET,
    SQLiteCloudConfig,
    SQLiteCloudConnect,
    SQLiteCloudNumber,
    SQLiteCloudRowsetSignature,
    SQLiteCloudValue,
    SQLiteDataTypes,
)
from sqlitecloud.exceptions import (
    SQLiteCloudException,
    get_sqlitecloud_error_with_extended_code,
)
from sqlitecloud.resultset import (
    SQLITECLOUD_RESULT_TYPE,
    SQLiteCloudOperationResult,
    SQLiteCloudResult,
    SQLiteCloudResultSet,
)


class Driver:
    def __init__(self) -> None:
        # Used while parsing chunked rowset
        self._rowset: SQLiteCloudResult = None

    def connect(
        self, hostname: str, port: int, config: SQLiteCloudConfig
    ) -> SQLiteCloudConnect:
        """
        Connect to the SQLite Cloud server.

        Args:
            hostname (str): The hostname of the server.
            port (int): The port number of the server.
            config (SQLiteCloudConfig): The configuration for the connection.

        Returns:
            SQLiteCloudConnect: The connection object.

        Raises:
            SQLiteCloudException: If an error occurs while connecting the socket.
        """
        sock = self._internal_connect(hostname, port, config)

        connection = SQLiteCloudConnect()
        connection.config = config
        connection.socket = sock

        self._internal_config_apply(connection, config)

        return connection

    def disconnect(
        self, conn: SQLiteCloudConnect, only_main_socket: bool = False
    ) -> None:
        """
        Disconnect from the SQLite Cloud server.
        """
        try:
            if conn.socket:
                conn.socket.close()
            if not only_main_socket and conn.pubsub_socket:
                conn.pubsub_socket.close()
        finally:
            conn.socket = None
            if not only_main_socket:
                conn.pubsub_socket = None

    def execute(
        self, command: str, connection: SQLiteCloudConnect
    ) -> SQLiteCloudResult:
        """
        Execute a command on the SQLite Cloud server.
        """
        command = self._internal_serialize_command(command)

        return self._internal_run_command(connection, command)

    def execute_statement(
        self,
        query: str,
        bindings: Tuple[SQLiteDataTypes],
        # bindings: Union[Tuple[SQLiteDataTypes], Dict[str, SQLiteDataTypes]],
        connection: SQLiteCloudConnect,
    ) -> Union[SQLiteCloudResult, SQLiteCloudOperationResult]:
        """
        Execute the statement on the SQLite Cloud server.
        It supports only the `qmark` style for parameter binding.
        """
        command = self._internal_serialize_command(
            [query] + list(bindings), zero_string=True
        )

        result = self._internal_run_command(connection, command)

        if result.tag != SQLITECLOUD_RESULT_TYPE.RESULT_ARRAY:
            return result

        return SQLiteCloudOperationResult(result)

    def send_blob(self, blob: bytes, conn: SQLiteCloudConnect) -> SQLiteCloudResult:
        """
        Send a blob to the SQLite Cloud server.
        """
        return self._internal_run_command(conn, self._internal_serialize_command(blob))

    def is_connected(
        self, connection: SQLiteCloudConnect, main_socket: bool = True
    ) -> bool:
        """
        Check if the connection is still open.
        """
        sock = connection.socket if main_socket else connection.pubsub_socket

        if not sock:
            return False
        try:
            sock.sendall(b"")
        except OSError:
            return False

        return True

    def _internal_connect(
        self, hostname: str, port: int, config: SQLiteCloudConfig
    ) -> socket:
        """
        Create a socket connection to the SQLite Cloud server.
        """
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(config.connect_timeout)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

        if not config.insecure:
            context = ssl.create_default_context(cafile=config.root_certificate)
            if config.certificate:
                context.load_cert_chain(
                    certfile=config.certificate, keyfile=config.certificate_key
                )
            if config.no_verify_certificate:
                context.check_hostname = False
                context.verify_mode = ssl.CERT_NONE

            sock = context.wrap_socket(sock, server_hostname=hostname)

        try:
            sock.connect((hostname, port))
        except Exception as e:
            errmsg = "An error occurred while initializing the socket."
            raise SQLiteCloudException(errmsg) from e

        return sock

    def _internal_reconnect(self, buffer: bytes) -> bool:
        return True

    def _internal_setup_pubsub(
        self, connection: SQLiteCloudConnect, buffer: bytes
    ) -> bool:
        """
        Prepare the connection for PubSub.
        Opens a new specific socket and starts the thread to listen for incoming messages.
        """
        if self.is_connected(connection, False):
            return True

        if connection.pubsub_callback is None:
            raise SQLiteCloudException(
                "A callback function must be provided to setup the PubSub connection."
            )

        connection.pubsub_socket = self._internal_connect(
            connection.config.account.hostname,
            connection.config.account.port,
            connection.config,
        )

        self._internal_run_command(
            connection, self._internal_serialize_command(buffer.decode()), False
        )
        thread = threading.Thread(
            target=self._internal_pubsub_thread, args=(connection,)
        )
        # kill the thread when the main one is terminated
        thread.daemon = True
        thread.start()
        connection.pubsub_thread = thread

        return True

    def _internal_pubsub_thread(self, connection: SQLiteCloudConnect) -> None:
        blen = 2048
        buffer: bytes = b""
        tread = 0

        try:
            while True:

                try:
                    if not connection.pubsub_socket:
                        logging.info("PubSub socket dismissed.")
                        break

                    # wait for the socket to be readable (no timeout)
                    ready_to_read, _, errors = select.select(
                        [connection.pubsub_socket], [], []
                    )
                    # eg, if the socket is closed
                    if len(errors) > 0:
                        break
                    # eg, no data to read
                    if len(ready_to_read) == 0:
                        continue

                    data = connection.pubsub_socket.recv(blen)
                    if not data:
                        logging.info("PubSub connection closed.")
                        break
                except Exception as e:
                    logging.error(
                        f"An error occurred while reading data: {SQLITECLOUD_INTERNAL_ERRCODE.NETWORK.value} ({e})."
                    )
                    break

                nread = len(data)
                tread += nread
                buffer += data

                sqlitecloud_number = self._internal_parse_number(buffer)
                clen = sqlitecloud_number.value
                if clen == 0:
                    continue

                # check if read is complete
                # clen is the lenght parsed in the buffer
                # cstart is the index of the first space
                cstart = sqlitecloud_number.cstart
                if clen + cstart != tread:
                    continue

                result = self._internal_parse_buffer(connection, buffer, tread)
                if result.tag == SQLITECLOUD_RESULT_TYPE.RESULT_STRING:
                    result.tag = SQLITECLOUD_RESULT_TYPE.RESULT_JSON

                connection.pubsub_callback(
                    connection, SQLiteCloudResultSet(result), connection.pubsub_data
                )

                # reset after having read the message
                tread = 0
                buffer: bytes = b""
        except Exception as e:
            logging.error(f"An error occurred while parsing data: {e}.")

        finally:
            if connection and connection.pubsub_callback:
                connection.pubsub_callback(connection, None, connection.pubsub_data)

    def upload_database(
        self,
        connection: SQLiteCloudConnect,
        dbname: str,
        key: Optional[str],
        is_file_transfer: bool,
        snapshot_id: int,
        is_internal_db: bool,
        fd: BufferedReader,
        dbsize: int,
        xCallback: Callable[[BufferedReader, int, int, int], bytes],
    ) -> None:
        """
        Uploads a database to the server.

        Args:
            connection (SQLiteCloudConnect): The connection object to the SQLite Cloud server.
            dbname (str): The name of the database to upload.
            key (Optional[str]): The encryption key for the database, if applicable.
            is_file_transfer (bool): Indicates whether the database is being transferred as a file.
            snapshot_id (int): The ID of the snapshot to upload.
            is_internal_db (bool): Indicates whether the database is an internal database.
            fd (BufferedReader): The file descriptor of the database file.
            dbsize (int): The size of the database file.
            xCallback (Callable[[BufferedReader, int, int, int], bytes]): The callback function to read the buffer.

        Raises:
            SQLiteCloudException: If an error occurs during the upload process.

        """
        keyarg = "KEY " if key else ""
        keyvalue = key if key else ""

        # prepare command to execute
        command = ""
        if is_file_transfer:
            internalarg = "INTERNAL" if is_internal_db else ""
            command = f"TRANSFER DATABASE '{dbname}' {keyarg}{keyvalue} SNAPSHOT {snapshot_id} {internalarg}"
        else:
            command = f"UPLOAD DATABASE '{dbname}' {keyarg}{keyvalue}"

        # execute command on server side
        result = self._internal_run_command(
            connection, self._internal_serialize_command(command)
        )
        if not result.data[0]:
            raise SQLiteCloudException(
                "An error occurred while initializing the upload of the database."
            )

        buffer: bytes = b""
        blen = 0
        nprogress = 0
        try:
            while True:
                # execute callback to read buffer
                blen = SQLITECLOUD_DEFAULT.UPLOAD_SIZE.value
                try:
                    buffer = xCallback(fd, blen, dbsize, nprogress)
                    blen = len(buffer)
                except Exception as e:
                    raise SQLiteCloudException(
                        "An error occurred while reading the file."
                    ) from e

                try:
                    # send also the final confirmation blob of zero bytes
                    self.send_blob(buffer, connection)
                except Exception as e:
                    raise SQLiteCloudException(
                        "An error occurred while uploading the file."
                    ) from e

                # update progress
                nprogress += blen

                if blen == 0:
                    # Upload completed
                    break
        except Exception as e:
            self._internal_run_command(
                connection, self._internal_serialize_command("UPLOAD ABORT")
            )
            raise e

    def download_database(
        self,
        connection: SQLiteCloudConnect,
        dbname: str,
        fd: BufferedWriter,
        xCallback: Callable[[BufferedWriter, int, int, int], bytes],
        if_exists: bool,
    ) -> None:
        """
        Downloads a database from the SQLite Cloud service.

        Args:
            connection (SQLiteCloudConnect): The connection object used to communicate with the SQLite Cloud service.
            dbname (str): The name of the database to download.
            fd (BufferedWriter): The file descriptor to write the downloaded data to.
            xCallback (Callable[[BufferedWriter, int, int, int], bytes]): A callback function to write downloaded data with the download progress information.
            if_exists (bool): If True, the download won't rise an exception if database is missing.

        Raises:
            SQLiteCloudException: If an error occurs while downloading the database.

        """
        exists_cmd = " IF EXISTS" if if_exists else ""
        result = self._internal_run_command(
            connection,
            self._internal_serialize_command(
                f"DOWNLOAD DATABASE {dbname}{exists_cmd};"
            ),
        )

        if result.nrows == 0:
            raise SQLiteCloudException(
                "An error occurred while initializing the download of the database."
            )

        # result is an ARRAY (database size, number of pages, raft_index)
        download_info = result.data[0]
        db_size = int(download_info[0])

        # loop to download
        progress_size = 0

        try:
            while progress_size < db_size:
                result = self._internal_run_command(
                    connection, self._internal_serialize_command("DOWNLOAD STEP")
                )

                # res is BLOB, decode it
                data = result.data[0]
                data_len = len(data)

                # execute callback (with progress_size updated)
                progress_size += data_len
                xCallback(fd, data, data_len, db_size, progress_size)

                # check exit condition
                if data_len == 0:
                    break
        except Exception as e:
            self._internal_run_command(
                connection, self._internal_serialize_command("DOWNLOAD ABORT")
            )
            raise e

    def _internal_config_apply(
        self, connection: SQLiteCloudConnect, config: SQLiteCloudConfig
    ) -> None:
        if config.timeout > 0:
            connection.socket.settimeout(config.timeout)

        command = ""

        # it must be executed before authentication command
        if config.non_linearizable:
            command += "SET CLIENT KEY NONLINEARIZABLE TO 1;"

        if config.account.apikey:
            command += f"AUTH APIKEY {config.account.apikey};"
        elif config.account.token:
            command += f"AUTH TOKEN {config.account.token};"
        elif config.account.username and config.account.password:
            option = "HASH" if config.account.password_hashed else "PASSWORD"
            command += f"AUTH USER {config.account.username} {option} {config.account.password};"

        if config.account.dbname:
            if config.create and not config.memory:
                command += f"CREATE DATABASE {config.account.dbname} IF NOT EXISTS;"
            command += f"USE DATABASE {config.account.dbname};"

        if config.compression:
            command += "SET CLIENT KEY COMPRESSION TO 1;"

        if config.zerotext:
            command += "SET CLIENT KEY ZEROTEXT TO 1;"

        if config.noblob:
            command += "SET CLIENT KEY NOBLOB TO 1;"

        if config.maxdata:
            command += f"SET CLIENT KEY MAXDATA TO {config.maxdata};"

        if config.maxrows:
            command += f"SET CLIENT KEY MAXROWS TO {config.maxrows};"

        if config.maxrowset:
            command += f"SET CLIENT KEY MAXROWSET TO {config.maxrowset};"

        if len(command) > 0:
            self._internal_run_command(
                connection, self._internal_serialize_command(command)
            )

    def _internal_run_command(
        self,
        connection: SQLiteCloudConnect,
        command: bytes,
        main_socket: bool = True,
    ) -> SQLiteCloudResult:
        """Send serialized command to the server and read the response."""
        if not self.is_connected(connection, main_socket):
            raise SQLiteCloudException(
                "The connection is closed.",
                SQLITECLOUD_INTERNAL_ERRCODE.NETWORK,
            )

        self._internal_socket_write(connection, command, main_socket)
        return self._internal_socket_read(connection, main_socket)

    def _internal_socket_write(
        self,
        connection: SQLiteCloudConnect,
        command: bytes,
        main_socket: bool = True,
    ) -> None:
        """
        Write to the socket the command serialized with the SCSP protocol.

        Args:
            connection (SQLiteCloudConnect): The connection object to the SQLite Cloud server.
            command (bytes): The command to send.
            main_socket (bool): If True, write to the main socket, otherwise write to the pubsub socket.
        """
        # write buffer
        if len(command) == 0:
            return
        try:
            sock = connection.socket if main_socket else connection.pubsub_socket

            sock.sendall(command)
        except Exception as exc:
            raise SQLiteCloudException(
                "An error occurred while writing data.",
                SQLITECLOUD_INTERNAL_ERRCODE.NETWORK,
            ) from exc

    def _internal_socket_read(
        self, connection: SQLiteCloudConnect, main_socket: bool = True
    ) -> SQLiteCloudResult:
        """
        Read from the socket and parse the response.

        The buffer is stored as a string of bytes without decoding it with UTF-8.
        The dimensions (LEN) specified in the SCSP protocol are in bytes, while
        Python counts decoded strings in characters. This can cause issues when
        slicing the buffer into parts if there are special characters like "ò".
        """
        buffer = b""
        command_type = ""
        command_length_value = b""
        nread = 0

        sock = connection.socket if main_socket else connection.pubsub_socket

        # read the lenght of the command, eg:
        # ?LEN <command>, where `?` is any command type
        # _ for null command
        # :145 for integer command with value 145
        while True:
            try:
                data = sock.recv(1)
                if not data:
                    raise SQLiteCloudException(
                        "Incomplete response from server. Cannot read the command length."
                    )
            except Exception as exc:
                raise SQLiteCloudException(
                    "An error occurred while reading command length from the socket.",
                    SQLITECLOUD_INTERNAL_ERRCODE.NETWORK,
                ) from exc

            nread += len(data)
            buffer += data

            # first character is the type of the message
            if nread == 1:
                command_type = data.decode()
                continue

            # end of len value
            if data == b" ":
                break

            command_length_value += data

        if (
            command_type == SQLITECLOUD_CMD.INT.value
            or command_type == SQLITECLOUD_CMD.FLOAT.value
            or command_type == SQLITECLOUD_CMD.NULL.value
        ):
            return self._internal_parse_buffer(connection, buffer, len(buffer))

        command_length = int(command_length_value)

        # read the command
        nread = 0

        while nread < command_length:
            buffer_size = min(command_length - nread, 8192)

            try:
                data = sock.recv(buffer_size)
                if not data:
                    raise SQLiteCloudException(
                        "Incomplete response from server. Cannot read the command."
                    )
            except Exception as exc:
                raise SQLiteCloudException(
                    "An error occurred while reading the command from the socket.",
                    SQLITECLOUD_INTERNAL_ERRCODE.NETWORK,
                ) from exc

            nread += len(data)
            buffer += data

        return self._internal_parse_buffer(connection, buffer, len(buffer))

    def _internal_parse_number(
        self, buffer: bytes, index: int = 1
    ) -> SQLiteCloudNumber:
        sqlitecloud_number = SQLiteCloudNumber()
        sqlitecloud_number.value = 0
        extvalue = 0
        offcode = 0
        isext = 0
        blen = len(buffer)

        # from 1 to skip the first command type character
        for i in range(index, blen):
            c = chr(buffer[i])

            # check for optional extended error code (ERRCODE:EXTERRCODE:OFFCODE)
            if c == ":":
                isext += 1
                continue

            # check for end of value
            if c == " ":
                sqlitecloud_number.cstart = i + 1
                sqlitecloud_number.extcode = extvalue
                sqlitecloud_number.offcode = offcode
                return sqlitecloud_number

            val = int(c) if c.isdigit() else 0

            if isext == 1:
                # XERRCODE
                extvalue = (extvalue * 10) + val
            elif isext == 2:
                # OFFCODE
                offcode = (offcode * 10) + val
            else:
                # generic value or ERRCODE
                sqlitecloud_number.value = (sqlitecloud_number.value * 10) + val

        sqlitecloud_number.value = 0
        return sqlitecloud_number

    def _internal_parse_buffer(
        self, connection: SQLiteCloudConnect, buffer: bytes, blen: int
    ) -> SQLiteCloudResult:
        # possible return values:
        # True 	=> OK
        # False 	=> error
        # integer
        # double
        # string
        # list
        # object
        # None

        # check OK value
        if buffer == b"+2 OK":
            return SQLiteCloudResult(SQLITECLOUD_RESULT_TYPE.RESULT_OK, True)

        cmd = chr(buffer[0])

        # check for compressed result
        if cmd == SQLITECLOUD_CMD.COMPRESSED.value:
            buffer = self._internal_uncompress_data(buffer)
            if buffer is None:
                raise SQLiteCloudException(
                    f"An error occurred while decompressing the input buffer of len {blen}."
                )

            # buffer after decompression
            blen = len(buffer)
            cmd = chr(buffer[0])

        # first character contains command type
        if cmd in [
            SQLITECLOUD_CMD.ZEROSTRING.value,
            SQLITECLOUD_CMD.RECONNECT.value,
            SQLITECLOUD_CMD.PUBSUB.value,
            SQLITECLOUD_CMD.COMMAND.value,
            SQLITECLOUD_CMD.STRING.value,
            SQLITECLOUD_CMD.ARRAY.value,
            SQLITECLOUD_CMD.BLOB.value,
            SQLITECLOUD_CMD.JSON.value,
        ]:
            sqlite_number = self._internal_parse_number(buffer)
            len_ = sqlite_number.value
            cstart = sqlite_number.cstart
            if len_ == 0:
                return SQLiteCloudResult(SQLITECLOUD_RESULT_TYPE.RESULT_STRING, "")

            tag = SQLITECLOUD_RESULT_TYPE.RESULT_STRING

            if cmd == SQLITECLOUD_CMD.ZEROSTRING.value:
                len_ -= 1
            clone = buffer[cstart : cstart + len_]

            if cmd == SQLITECLOUD_CMD.COMMAND.value:
                return self._internal_run_command(
                    connection, self._internal_serialize_command(clone.decode())
                )
            elif cmd == SQLITECLOUD_CMD.PUBSUB.value:
                return SQLiteCloudResult(
                    SQLITECLOUD_RESULT_TYPE.RESULT_OK,
                    self._internal_setup_pubsub(connection, clone),
                )
            elif cmd == SQLITECLOUD_CMD.RECONNECT.value:
                return SQLiteCloudResult(
                    SQLITECLOUD_RESULT_TYPE.RESULT_OK, self._internal_reconnect(clone)
                )
            elif cmd == SQLITECLOUD_CMD.ARRAY.value:
                return SQLiteCloudResult(
                    SQLITECLOUD_RESULT_TYPE.RESULT_ARRAY,
                    self._internal_parse_array(clone),
                )
            elif cmd == SQLITECLOUD_CMD.BLOB.value:
                tag = SQLITECLOUD_RESULT_TYPE.RESULT_BLOB
            elif cmd == SQLITECLOUD_CMD.JSON.value:
                return SQLiteCloudResult(
                    SQLITECLOUD_RESULT_TYPE.RESULT_JSON, json.loads(clone)
                )

            clone = clone.decode() if cmd != SQLITECLOUD_CMD.BLOB.value else clone
            return SQLiteCloudResult(tag, clone)

        elif cmd == SQLITECLOUD_CMD.ERROR.value:
            # -LEN ERRCODE:EXTCODE:OFFCODE ERRMSG
            sqlite_number = self._internal_parse_number(buffer)
            len_ = sqlite_number.value
            cstart = sqlite_number.cstart
            clone = buffer[cstart:]

            sqlite_number = self._internal_parse_number(clone, 0)
            cstart2 = sqlite_number.cstart

            errcode = sqlite_number.value
            xerrcode = sqlite_number.extcode

            len_ -= cstart2
            errmsg = clone[cstart2:]

            raise get_sqlitecloud_error_with_extended_code(
                errmsg.decode(), errcode, xerrcode
            )(errmsg.decode(), errcode, xerrcode)

        elif cmd in [SQLITECLOUD_CMD.ROWSET.value, SQLITECLOUD_CMD.ROWSET_CHUNK.value]:
            # CMD_ROWSET:          *LEN 0:VERSION ROWS COLS DATA
            # - When decompressed, LEN for ROWSET is *0
            #
            # CMD_ROWSET_CHUNK:    /LEN IDX:VERSION ROWS COLS DATA
            #
            rowset_signature = self._internal_parse_rowset_signature(buffer)
            if rowset_signature.start < 0:
                raise SQLiteCloudException("Cannot parse rowset signature")

            # check for end-of-chunk condition
            if rowset_signature.start == 0 and rowset_signature.version == 0:
                rowset = self._rowset
                self._rowset = None
                return rowset

            rowset = self._internal_parse_rowset(
                buffer,
                rowset_signature.start,
                rowset_signature.idx,
                rowset_signature.version,
                rowset_signature.nrows,
                rowset_signature.ncols,
            )

            # continue reading from the socket
            # until the end-of-chunk condition
            if cmd == SQLITECLOUD_CMD.ROWSET_CHUNK.value:
                return self._internal_socket_read(connection)

            return rowset

        elif cmd == SQLITECLOUD_CMD.NULL.value:
            return SQLiteCloudResult(SQLITECLOUD_RESULT_TYPE.RESULT_NONE, None)

        elif cmd in [SQLITECLOUD_CMD.INT.value, SQLITECLOUD_CMD.FLOAT.value]:
            sqlitecloud_value = self._internal_parse_value(buffer)
            clone = sqlitecloud_value.value

            tag = (
                SQLITECLOUD_RESULT_TYPE.RESULT_INTEGER
                if cmd == SQLITECLOUD_CMD.INT.value
                else SQLITECLOUD_RESULT_TYPE.RESULT_FLOAT
            )

            if clone is None:
                return SQLiteCloudResult(tag, 0)

            if cmd == SQLITECLOUD_CMD.INT.value:
                return SQLiteCloudResult(tag, int(clone))
            return SQLiteCloudResult(tag, float(clone))

        elif cmd == SQLITECLOUD_CMD.RAWJSON.value:
            return SQLiteCloudResult(SQLITECLOUD_RESULT_TYPE.RESULT_NONE, None)

        return SQLiteCloudResult(SQLITECLOUD_RESULT_TYPE.RESULT_NONE, None)

    def _internal_uncompress_data(self, buffer: bytes) -> Optional[bytes]:
        """
        %LEN COMPRESSED UNCOMPRESSED BUFFER

        Args:
            buffer (str): The compressed data buffer.
            blen (int): The length of the buffer.

        Returns:
            str: The uncompressed data.
        """
        # buffer may contain a sequence of compressed data
        # eg, a compressed rowset split in chunks is a sequence of rowset chunks
        # compressed individually, each one with its compressed header,
        # rowset header and compressed data
        space_index = buffer.index(b" ")
        buffer = buffer[space_index + 1 :]

        # extract compressed size
        space_index = buffer.index(b" ")
        compressed_size = int(buffer[:space_index].decode("utf-8"))
        buffer = buffer[space_index + 1 :]

        # extract decompressed size
        space_index = buffer.index(b" ")
        uncompressed_size = int(buffer[:space_index].decode("utf-8"))
        buffer = buffer[space_index + 1 :]

        # extract data header
        header = buffer[:-compressed_size]

        # extract compressed data
        compressed_buffer = buffer[-compressed_size:]

        decompressed_buffer = header + lz4.block.decompress(
            compressed_buffer, uncompressed_size
        )

        # sanity check result
        if len(decompressed_buffer) != uncompressed_size + len(header):
            return None

        return decompressed_buffer

    def _internal_parse_array(self, buffer: bytes) -> list:
        start = 0
        sqlite_number = self._internal_parse_number(buffer, start)
        n = sqlite_number.value
        start = sqlite_number.cstart

        r: str = []
        for i in range(n):
            sqlitecloud_value = self._internal_parse_value(buffer, start)
            start += sqlitecloud_value.cellsize
            r.append(sqlitecloud_value.value)

        return r

    def _internal_parse_value(self, buffer: bytes, index: int = 0) -> SQLiteCloudValue:
        sqlitecloud_value = SQLiteCloudValue()
        len = 0
        cellsize = 0

        # handle special NULL value case
        c = chr(buffer[index])
        if buffer is None or c == SQLITECLOUD_CMD.NULL.value:
            len = 0
            if cellsize is not None:
                cellsize = 2

            sqlitecloud_value.value = None
            sqlitecloud_value.len = len
            sqlitecloud_value.cellsize = cellsize

            return sqlitecloud_value

        sqlitecloud_number = self._internal_parse_number(buffer, index + 1)
        blen = sqlitecloud_number.value
        cstart = sqlitecloud_number.cstart

        # handle decimal/float cases
        if c == SQLITECLOUD_CMD.INT.value or c == SQLITECLOUD_CMD.FLOAT.value:
            nlen = cstart - index
            len = nlen - 2
            cellsize = nlen

            value = (buffer[index + 1 : index + 1 + len]).decode()

            sqlitecloud_value.value = (
                int(value) if c == SQLITECLOUD_CMD.INT.value else float(value)
            )
            sqlitecloud_value.len
            sqlitecloud_value.cellsize = cellsize

            return sqlitecloud_value

        len = blen - 1 if c == SQLITECLOUD_CMD.ZEROSTRING.value else blen
        cellsize = blen + cstart - index

        value = buffer[cstart : cstart + len]

        if c == SQLITECLOUD_CMD.STRING.value or c == SQLITECLOUD_CMD.ZEROSTRING.value:
            value = value.decode()

        sqlitecloud_value.value = value
        sqlitecloud_value.len = len
        sqlitecloud_value.cellsize = cellsize

        return sqlitecloud_value

    def _internal_parse_rowset_signature(
        self, buffer: bytes
    ) -> SQLiteCloudRowsetSignature:
        # ROWSET:          *LEN 0:VERS NROWS NCOLS DATA
        # ROWSET in CHUNK: /LEN IDX:VERS NROWS NCOLS DATA

        signature = SQLiteCloudRowsetSignature()

        # check for end-of-chunk condition
        if buffer == SQLITECLOUD_ROWSET.CHUNKS_END.value:
            signature.version = 0
            signature.start = 0
            return signature

        start = 1
        counter = 0
        n = len(buffer)
        for i in range(n):
            if chr(buffer[i]) != " ":
                continue
            counter += 1

            data = (buffer[start:i]).decode()
            start = i + 1

            if counter == 1:
                signature.len = int(data)
            elif counter == 2:
                # idx:vers
                values = data.split(":")
                signature.idx = int(values[0])
                signature.version = int(values[1])
            elif counter == 3:
                signature.nrows = int(data)
            elif counter == 4:
                signature.ncols = int(data)

                signature.start = start

                return signature
            else:
                return SQLiteCloudRowsetSignature()
        return SQLiteCloudRowsetSignature()

    def _internal_parse_rowset(
        self, buffer: bytes, start: int, idx: int, version: int, nrows: int, ncols: int
    ) -> SQLiteCloudResult:
        rowset = None
        n = start
        ischunk = chr(buffer[0]) == SQLITECLOUD_CMD.ROWSET_CHUNK.value

        # idx == 0 means first (and only) chunk for rowset
        # idx == 1 means first chunk for chunked rowset
        first_chunk = (ischunk and idx == 1) or (not ischunk and idx == 0)
        if first_chunk:
            rowset = SQLiteCloudResult(SQLITECLOUD_RESULT_TYPE.RESULT_ROWSET)
            rowset.nrows = nrows
            rowset.ncols = ncols
            rowset.version = version
            rowset.data = []
            if ischunk:
                self._rowset = rowset
            n = self._internal_parse_rowset_header(rowset, buffer, start)
            if n <= 0:
                raise SQLiteCloudException("Cannot parse rowset header")
        else:
            rowset = self._rowset
            rowset.nrows += nrows

        # parse values
        self._internal_parse_rowset_values(rowset, buffer, n, nrows * ncols)

        return rowset

    def _internal_parse_rowset_header(
        self, rowset: SQLiteCloudResult, buffer: bytes, start: int
    ) -> int:
        ncols = rowset.ncols

        # parse column names
        rowset.colname = []
        for i in range(ncols):
            sqlitecloud_number = self._internal_parse_number(buffer, start)
            number_len = sqlitecloud_number.value
            cstart = sqlitecloud_number.cstart
            value = buffer[cstart : cstart + number_len]
            rowset.colname.append(value.decode())
            start = cstart + number_len

        if rowset.version == 1:
            return start

        if rowset.version != 2:
            raise SQLiteCloudException(
                f"Rowset version {rowset.version} is not supported."
            )

        # parse declared types
        rowset.decltype = []
        for i in range(ncols):
            sqlitecloud_number = self._internal_parse_number(buffer, start)
            number_len = sqlitecloud_number.value
            cstart = sqlitecloud_number.cstart
            value = buffer[cstart : cstart + number_len]
            rowset.decltype.append(value.decode())
            start = cstart + number_len

        # parse database names
        rowset.dbname = []
        for i in range(ncols):
            sqlitecloud_number = self._internal_parse_number(buffer, start)
            number_len = sqlitecloud_number.value
            cstart = sqlitecloud_number.cstart
            value = buffer[cstart : cstart + number_len]
            rowset.dbname.append(value.decode())
            start = cstart + number_len

        # parse table names
        rowset.tblname = []
        for i in range(ncols):
            sqlitecloud_number = self._internal_parse_number(buffer, start)
            number_len = sqlitecloud_number.value
            cstart = sqlitecloud_number.cstart
            value = buffer[cstart : cstart + number_len]
            rowset.tblname.append(value.decode())
            start = cstart + number_len

        # parse column original names
        rowset.origname = []
        for i in range(ncols):
            sqlitecloud_number = self._internal_parse_number(buffer, start)
            number_len = sqlitecloud_number.value
            cstart = sqlitecloud_number.cstart
            value = buffer[cstart : cstart + number_len]
            rowset.origname.append(value.decode())
            start = cstart + number_len

        # parse not null flags
        rowset.notnull = []
        for i in range(ncols):
            sqlitecloud_number = self._internal_parse_number(buffer, start)
            rowset.notnull.append(sqlitecloud_number.value)
            start = sqlitecloud_number.cstart

        # parse primary key flags
        rowset.prikey = []
        for i in range(ncols):
            sqlitecloud_number = self._internal_parse_number(buffer, start)
            rowset.prikey.append(sqlitecloud_number.value)
            start = sqlitecloud_number.cstart

        # parse autoincrement flags
        rowset.autoinc = []
        for i in range(ncols):
            sqlitecloud_number = self._internal_parse_number(buffer, start)
            rowset.autoinc.append(sqlitecloud_number.value)
            start = sqlitecloud_number.cstart

        return start

    def _internal_parse_rowset_values(
        self, rowset: SQLiteCloudResult, buffer: bytes, start: int, bound: int
    ):
        # loop to parse each individual value
        for i in range(bound):
            sqlitecloud_value = self._internal_parse_value(buffer, start)
            start += sqlitecloud_value.cellsize
            rowset.data.append(sqlitecloud_value.value)

    def _internal_serialize_command(
        self, data: Any, zero_string: bool = False
    ) -> bytes:
        if isinstance(data, str):
            cmd = SQLITECLOUD_CMD.STRING.value
            if zero_string:
                cmd = SQLITECLOUD_CMD.ZEROSTRING.value
                data += "\x00"

            data = data.encode()
            header = f"{cmd}{len(data)} ".encode()

            return header + data

        if isinstance(data, int):
            return f"{SQLITECLOUD_CMD.INT.value}{data} ".encode()

        if isinstance(data, float):
            return f"{SQLITECLOUD_CMD.FLOAT.value}{data} ".encode()

        if isinstance(data, bytes):
            header = f"{SQLITECLOUD_CMD.BLOB.value}{len(data)} ".encode()
            return header + data

        if data is None:
            return f"{SQLITECLOUD_CMD.NULL.value} ".encode()

        if isinstance(data, List):
            return self._internal_serialize_array(data, zero_string=zero_string)

        raise SQLiteCloudException(
            f"Unsupported data type for serialization: {type(data)}"
        )

    def _internal_serialize_array(self, data: List, zero_string: bool = False) -> str:
        n = len(data)
        serialized_data: bytes = f"{n} ".encode()
        for i in range(n):
            # the query must be zero-terminated
            zs = i == 0 or zero_string
            serialized_data += self._internal_serialize_command(data[i], zero_string=zs)

        header = f"{SQLITECLOUD_CMD.ARRAY.value}{len(serialized_data)} ".encode()

        return header + serialized_data
