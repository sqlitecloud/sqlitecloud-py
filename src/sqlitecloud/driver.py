import ssl
from typing import Optional, Union
import lz4.block
from sqlitecloud.lz4_custom import lz4decode
from sqlitecloud.resultset import SQCloudResult
from sqlitecloud.types import (
    SQCLOUD_CMD,
    SQCLOUD_INTERNAL_ERRCODE,
    SQCLOUD_ROWSET,
    SQCloudConfig,
    SQCloudConnect,
    SQCloudException,
    SQCloudNumber,
    SQCloudRowsetSignature,
    SQCloudValue,
)
import socket


class Driver:
    def __init__(self) -> None:
        # Used while parsing chunked rowset
        self._rowset: SQCloudResult = None

    def connect(
        self, hostname: str, port: int, config: SQCloudConfig
    ) -> SQCloudConnect:
        """
        Connects to the SQLite Cloud server.

        Args:
            hostname (str): The hostname of the server.
            port (int): The port number of the server.
            config (SQCloudConfig): The configuration for the connection.

        Returns:
            SQCloudConnect: The connection object.

        Raises:
            SQCloudException: If an error occurs while initializing the socket.
        """
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(config.connect_timeout)

        if not config.insecure:
            context = ssl.create_default_context(cafile=config.root_certificate)
            if config.certificate:
                context.load_cert_chain(
                    certfile=config.certificate, keyfile=config.certificate_key
                )

            sock = context.wrap_socket(sock, server_hostname=hostname)

        try:
            sock.connect((hostname, port))
        except Exception as e:
            errmsg = f"An error occurred while initializing the socket."
            raise SQCloudException(errmsg) from e

        connection = SQCloudConnect()
        connection.socket = sock
        connection.config = config

        self._internal_config_apply(connection, config)

        return connection

    def disconnect(self, conn: SQCloudConnect):
        try:
            if conn.socket:
                conn.socket.close()
        finally:
            conn.socket = None

    def execute(self, command: str, connection: SQCloudConnect) -> SQCloudResult:
        return self._internal_run_command(connection, command)

    def sendblob(self, blob: bytes, conn: SQCloudConnect) -> SQCloudResult:
        try:
            conn.isblob = True
            return self._internal_run_command(conn, blob)
        finally:
            conn.isblob = False

    def _internal_reconnect(self, buffer: bytes) -> bool:
        return True

    def _internal_setup_pubsub(self, buffer: bytes) -> bool:
        return True

    def _internal_config_apply(
        self, connection: SQCloudConnect, config: SQCloudConfig
    ) -> None:
        if config.timeout > 0:
            connection.socket.settimeout(config.timeout)

        buffer = ""

        if config.account.apikey:
            buffer += f"AUTH APIKEY {config.account.apikey};"

        if config.account.username and config.account.password:
            command = "HASH" if config.account.password_hashed else "PASSWORD"
            buffer += f"AUTH USER {config.account.username} {command} {config.account.password};"

        if config.account.database:
            if config.create and not config.memory:
                buffer += f"CREATE DATABASE {config.account.database} IF NOT EXISTS;"
            buffer += f"USE DATABASE {config.account.database};"

        if config.compression:
            buffer += "SET CLIENT KEY COMPRESSION TO 1;"

        if config.zerotext:
            buffer += "SET CLIENT KEY ZEROTEXT TO 1;"

        if config.non_linearizable:
            buffer += "SET CLIENT KEY NONLINEARIZABLE TO 1;"

        if config.noblob:
            buffer += "SET CLIENT KEY NOBLOB TO 1;"

        if config.maxdata:
            buffer += f"SET CLIENT KEY MAXDATA TO {config.maxdata};"

        if config.maxrows:
            buffer += f"SET CLIENT KEY MAXROWS TO {config.maxrows};"

        if config.maxrowset:
            buffer += f"SET CLIENT KEY MAXROWSET TO {config.maxrowset};"

        if len(buffer) > 0:
            self._internal_run_command(connection, buffer)

    def _internal_run_command(
        self, connection: SQCloudConnect, command: Union[str, bytes]
    ) -> None:
        self._internal_socket_write(connection, command)
        return self._internal_socket_read(connection)

    def _internal_socket_write(
        self, connection: SQCloudConnect, command: Union[str, bytes]
    ) -> None:
        # compute header
        delimit = "$" if connection.isblob else "+"
        buffer = command.encode() if isinstance(command, str) else command
        buffer_len = len(buffer)
        header = f"{delimit}{buffer_len} "

        # write header
        try:
            connection.socket.sendall(header.encode())
        except Exception as exc:
            raise SQCloudException(
                "An error occurred while writing header data.",
                SQCLOUD_INTERNAL_ERRCODE.INTERNAL_ERRCODE_NETWORK,
            ) from exc

        # write buffer
        if buffer_len == 0:
            return
        try:
            connection.socket.sendall(buffer)
        except Exception as exc:
            raise SQCloudException(
                "An error occurred while writing data.",
                SQCLOUD_INTERNAL_ERRCODE.INTERNAL_ERRCODE_NETWORK,
            ) from exc

    def _internal_socket_read(self, connection: SQCloudConnect) -> SQCloudResult:
        """
        Read from the socket and parse the response.

        The buffer is stored as a string of bytes without decoding it with UTF-8.
        The dimensions (LEN) specified in the SCSP protocol are in bytes, while
        Python counts decoded strings in characters. This can cause issues when
        slicing the buffer into parts if there are special characters like "ò".
        """
        buffer = b""
        buffer_size = 8192
        nread = 0

        while True:
            try:
                data = connection.socket.recv(buffer_size)
                if not data:
                    raise SQCloudException("Incomplete response from server.")
            except Exception as exc:
                raise SQCloudException(
                    "An error occurred while reading data from the socket.",
                    SQCLOUD_INTERNAL_ERRCODE.INTERNAL_ERRCODE_NETWORK,
                ) from exc

            # the expected data length to read
            # matches the string size before decoding it
            nread += len(data)
            # update buffers
            buffer += data

            c = chr(buffer[0])

            if (
                c == SQCLOUD_CMD.INT.value
                or c == SQCLOUD_CMD.FLOAT.value
                or c == SQCLOUD_CMD.NULL.value
            ):
                if not buffer.endswith(b" "):
                    continue
            elif c == SQCLOUD_CMD.ROWSET_CHUNK.value:
                isEndOfChunk = buffer.endswith(SQCLOUD_ROWSET.CHUNKS_END.value)
                if not isEndOfChunk:
                    continue
            else:
                sqcloud_number = self._internal_parse_number(buffer)
                n = sqcloud_number.value
                cstart = sqcloud_number.cstart

                can_be_zerolength = (
                    c == SQCLOUD_CMD.BLOB.value or c == SQCLOUD_CMD.STRING.value
                )
                if n == 0 and not can_be_zerolength:
                    continue
                if n + cstart != nread:
                    continue

            return self._internal_parse_buffer(connection, buffer, len(buffer))

    def _internal_parse_number(self, buffer: bytes, index: int = 1) -> SQCloudNumber:
        sqcloud_number = SQCloudNumber()
        sqcloud_number.value = 0
        extvalue = 0
        isext = False
        blen = len(buffer)

        # from 1 to skip the first command type character
        for i in range(index, blen):
            c = chr(buffer[i])

            # check for optional extended error code (ERRCODE:EXTERRCODE)
            if c == ":":
                isext = True
                continue

            # check for end of value
            if c == " ":
                sqcloud_number.cstart = i + 1
                sqcloud_number.extcode = extvalue
                return sqcloud_number

            val = int(c) if c.isdigit() else 0

            # compute numeric value
            if isext:
                extvalue = (extvalue * 10) + val
            else:
                sqcloud_number.value = (sqcloud_number.value * 10) + val

        sqcloud_number.value = 0
        return sqcloud_number

    def _internal_parse_buffer(
        self, connection: SQCloudConnect, buffer: bytes, blen: int
    ) -> SQCloudResult:
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
            return SQCloudResult(True)

        cmd = chr(buffer[0])

        # check for compressed result
        if cmd == SQCLOUD_CMD.COMPRESSED.value:
            buffer = self._internal_uncompress_data(buffer, blen)
            if buffer is None:
                raise SQCloudException(
                    f"An error occurred while decompressing the input buffer of len {blen}."
                )

        # first character contains command type
        if cmd in [
            SQCLOUD_CMD.ZEROSTRING.value,
            SQCLOUD_CMD.RECONNECT.value,
            SQCLOUD_CMD.PUBSUB.value,
            SQCLOUD_CMD.COMMAND.value,
            SQCLOUD_CMD.STRING.value,
            SQCLOUD_CMD.ARRAY.value,
            SQCLOUD_CMD.BLOB.value,
            SQCLOUD_CMD.JSON.value,
        ]:
            sqlite_number = self._internal_parse_number(buffer)
            len_ = sqlite_number.value
            cstart = sqlite_number.cstart
            if len_ == 0:
                return SQCloudResult("")

            if cmd == SQCLOUD_CMD.ZEROSTRING.value:
                len_ -= 1
            clone = buffer[cstart : cstart + len_]

            if cmd == SQCLOUD_CMD.COMMAND.value:
                return self._internal_run_command(connection, clone)
            elif cmd == SQCLOUD_CMD.PUBSUB.value:
                return SQCloudResult(self._internal_setup_pubsub(clone))
            elif cmd == SQCLOUD_CMD.RECONNECT.value:
                return SQCloudResult(self._internal_reconnect(clone))
            elif cmd == SQCLOUD_CMD.ARRAY.value:
                return SQCloudResult(self._internal_parse_array(clone))

            clone = clone.decode() if cmd != SQCLOUD_CMD.BLOB.value else clone
            return SQCloudResult(clone)

        elif cmd == SQCLOUD_CMD.ERROR.value:
            # -LEN ERRCODE:EXTCODE ERRMSG
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

            raise SQCloudException(errmsg.decode(), errcode, xerrcode)

        elif cmd in [SQCLOUD_CMD.ROWSET.value, SQCLOUD_CMD.ROWSET_CHUNK.value]:
            # CMD_ROWSET:          *LEN 0:VERSION ROWS COLS DATA
            # CMD_ROWSET_CHUNK:    /LEN IDX:VERSION ROWS COLS DATA
            rowset_signature = self._internal_parse_rowset_signature(buffer)
            if rowset_signature.start < 0:
                raise SQCloudException("Cannot parse rowset signature")

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

            # continue parsing next chunk in the buffer
            sign_len = rowset_signature.len
            buffer = buffer[sign_len + len(f"/{sign_len} ") :]
            if buffer:
                return self._internal_parse_buffer(connection, buffer, len(buffer))

            return rowset

        elif cmd == SQCLOUD_CMD.NULL.value:
            return None

        elif cmd in [SQCLOUD_CMD.INT.value, SQCLOUD_CMD.FLOAT.value]:
            sqcloud_value = self._internal_parse_value(buffer)
            clone = sqcloud_value.value

            if clone is None:
                return SQCloudResult(0)

            if cmd == SQCLOUD_CMD.INT.value:
                return SQCloudResult(int(clone))
            return SQCloudResult(float(clone))

        elif cmd == SQCLOUD_CMD.RAWJSON.value:
            # TODO: isn't implemented in C?
            return SQCloudResult(None)

        # TODO: exception here?
        return SQCloudResult(None)

    def _internal_uncompress_data(self, buffer: bytes, blen: int) -> Optional[bytes]:
        """
        %LEN COMPRESSED UNCOMPRESSED BUFFER

        Args:
            buffer (str): The compressed data buffer.
            blen (int): The length of the buffer.

        Returns:
            str: The uncompressed data.
        """
        tlen = 0  # total length
        clen = 0  # compressed length
        ulen = 0  # uncompressed length
        hlen = 0  # raw header length
        seek1 = 0

        start = 1
        counter = 0
        for i in range(blen):
            if chr(buffer[i]) != " ":
                continue
            counter += 1

            data = buffer[start:i]
            start = i + 1

            if counter == 1:
                tlen = int(data)
                seek1 = start
            elif counter == 2:
                clen = int(data)
            elif counter == 3:
                ulen = int(data)
                break

        # sanity check header values
        if tlen == 0 or clen == 0 or ulen == 0 or start == 1 or seek1 == 0:
            return None

        # copy raw header
        hlen = start - seek1
        header = buffer[start : start + hlen]

        # compute index of the first compressed byte
        start += hlen

        # perform real decompression
        # clone = header + lz4.block.decompress(buffer[start:])
        clone = lz4decode(buffer, start, header)

        # sanity check result
        if len(clone) != ulen + hlen:
            return None

        return clone

    def _internal_parse_array(self, buffer: bytes) -> list:
        start = 0
        sqlite_number = self._internal_parse_number(buffer, start)
        n = sqlite_number.value
        start = sqlite_number.cstart

        r: str = []
        for i in range(n):
            sqcloud_value = self._internal_parse_value(buffer, start)
            start += sqcloud_value.cellsize
            r.append(sqcloud_value.value)

        return r

    def _internal_parse_value(self, buffer: bytes, index: int = 0) -> SQCloudValue:
        sqcloud_value = SQCloudValue()
        len = 0
        cellsize = 0

        # handle special NULL value case
        c = chr(buffer[index])
        if buffer is None or c == SQCLOUD_CMD.NULL.value:
            len = 0
            if cellsize is not None:
                cellsize = 2

            sqcloud_value.len = len
            sqcloud_value.cellsize = cellsize

            return sqcloud_value

        sqcloud_number = self._internal_parse_number(buffer, index + 1)
        blen = sqcloud_number.value
        cstart = sqcloud_number.cstart

        # handle decimal/float cases
        if c == SQCLOUD_CMD.INT.value or c == SQCLOUD_CMD.FLOAT.value:
            nlen = cstart - index
            len = nlen - 2
            cellsize = nlen

            sqcloud_value.value = (buffer[index + 1 : index + 1 + len]).decode()
            sqcloud_value.len
            sqcloud_value.cellsize = cellsize

            return sqcloud_value

        len = blen - 1 if c == SQCLOUD_CMD.ZEROSTRING.value else blen
        cellsize = blen + cstart - index

        sqcloud_value.value = (buffer[cstart : cstart + len]).decode()
        sqcloud_value.len = len
        sqcloud_value.cellsize = cellsize

        return sqcloud_value

    def _internal_parse_rowset_signature(self, buffer: bytes) -> SQCloudRowsetSignature:
        # ROWSET:          *LEN 0:VERS NROWS NCOLS DATA
        # ROWSET in CHUNK: /LEN IDX:VERS NROWS NCOLS DATA

        signature = SQCloudRowsetSignature()

        # check for end-of-chunk condition
        if buffer == SQCLOUD_ROWSET.CHUNKS_END.value:
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
                return SQCloudRowsetSignature()
        return SQCloudRowsetSignature()

    def _internal_parse_rowset(
        self, buffer: bytes, start: int, idx: int, version: int, nrows: int, ncols: int
    ) -> SQCloudResult:
        rowset = None
        n = start
        ischunk = chr(buffer[0]) == SQCLOUD_CMD.ROWSET_CHUNK.value

        # idx == 0 means first (and only) chunk for rowset
        # idx == 1 means first chunk for chunked rowset
        first_chunk = (ischunk and idx == 1) or (not ischunk and idx == 0)
        if first_chunk:
            rowset = SQCloudResult()
            rowset.nrows = nrows
            rowset.ncols = ncols
            rowset.version = version
            rowset.data = []
            if ischunk:
                self._rowset = rowset
            n = self._internal_parse_rowset_header(rowset, buffer, start)
            if n <= 0:
                raise SQCloudException("Cannot parse rowset header")
        else:
            rowset = self._rowset
            rowset.nrows += nrows

        # parse values
        self._internal_parse_rowset_values(rowset, buffer, n, nrows * ncols)

        return rowset

    def _internal_parse_rowset_header(
        self, rowset: SQCloudResult, buffer: bytes, start: int
    ) -> int:
        ncols = rowset.ncols

        # parse column names
        rowset.colname = []
        for i in range(ncols):
            sqcloud_number = self._internal_parse_number(buffer, start)
            number_len = sqcloud_number.value
            cstart = sqcloud_number.cstart
            value = buffer[cstart : cstart + number_len]
            rowset.colname.append(value.decode())
            start = cstart + number_len

        if rowset.version == 1:
            return start

        if rowset.version != 2:
            raise SQCloudException(f"Rowset version {rowset.version} is not supported.")

        # parse declared types
        rowset.decltype = []
        for i in range(ncols):
            sqcloud_number = self._internal_parse_number(buffer, start)
            number_len = sqcloud_number.value
            cstart = sqcloud_number.cstart
            value = buffer[cstart : cstart + number_len]
            rowset.decltype.append(value.decode())
            start = cstart + number_len

        # parse database names
        rowset.dbname = []
        for i in range(ncols):
            sqcloud_number = self._internal_parse_number(buffer, start)
            number_len = sqcloud_number.value
            cstart = sqcloud_number.cstart
            value = buffer[cstart : cstart + number_len]
            rowset.dbname.append(value.decode())
            start = cstart + number_len

        # parse table names
        rowset.tblname = []
        for i in range(ncols):
            sqcloud_number = self._internal_parse_number(buffer, start)
            number_len = sqcloud_number.value
            cstart = sqcloud_number.cstart
            value = buffer[cstart : cstart + number_len]
            rowset.tblname.append(value.decode())
            start = cstart + number_len

        # parse column original names
        rowset.origname = []
        for i in range(ncols):
            sqcloud_number = self._internal_parse_number(buffer, start)
            number_len = sqcloud_number.value
            cstart = sqcloud_number.cstart
            value = buffer[cstart : cstart + number_len]
            rowset.origname.append(value.decode())
            start = cstart + number_len

        # parse not null flags
        rowset.notnull = []
        for i in range(ncols):
            sqcloud_number = self._internal_parse_number(buffer, start)
            rowset.notnull.append(sqcloud_number.value)
            start = sqcloud_number.cstart

        # parse primary key flags
        rowset.prikey = []
        for i in range(ncols):
            sqcloud_number = self._internal_parse_number(buffer, start)
            rowset.prikey.append(sqcloud_number.value)
            start = sqcloud_number.cstart

        # parse autoincrement flags
        rowset.autoinc = []
        for i in range(ncols):
            sqcloud_number = self._internal_parse_number(buffer, start)
            rowset.autoinc.append(sqcloud_number.value)
            start = sqcloud_number.cstart

        return start

    def _internal_parse_rowset_values(
        self, rowset: SQCloudResult, buffer: bytes, start: int, bound: int
    ):
        # loop to parse each individual value
        for i in range(bound):
            sqcloud_value = self._internal_parse_value(buffer, start)
            start += sqcloud_value.cellsize
            rowset.data.append(sqcloud_value.value)
