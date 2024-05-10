import ssl
from typing import Optional
from sqlitecloud.types import (
    SQCLOUD_CMD,
    SQCLOUD_INTERNAL_ERRCODE,
    SQCloudConfig,
    SQCloudConnect,
    SQCloudException,
    SQCloudNumber,
)
import socket


class Driver:
    def connect(self, hostname: str, port: int, config: SQCloudConfig) -> SQCloudConnect:
        """
        Connects to the SQLite Cloud server.

        Args:
            hostname (str, optional): The hostname of the server. Defaults to "localhost".
            port (int, optional): The port number of the server. Defaults to 8860.
            config (SQCloudConfig, optional): The configuration for the connection. Defaults to None.

        Returns:
            SQCloudConnect: The connection object.

        Raises:
            SQCloudException: If an error occurs while initializing the socket.
        """
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(config.connect_timeout)

        if not config.insecure:
            context = ssl.create_default_context(cafile=config.tls_root_certificate)
            if config.tls_certificate:
                context.load_cert_chain(
                    certfile=config.tls_certificate, keyfile=config.tls_certificate_key
                )

            sock = context.wrap_socket(sock, server_hostname=hostname)

        try:
            sock.connect((hostname, port))
        except Exception as e:
            errmsg = f"An error occurred while initializing the socket."
            raise SQCloudException(errmsg, -1, exception=e)

        connection = SQCloudConnect()
        connection.socket = sock
        connection.config = config

        self._internal_config_apply(connection, config)

        return connection

    def disconnect(self, conn: SQCloudConnect):
        if conn.socket:
            conn.socket.close()
        conn.socket = None

    def execute(self):
        pass

    def sendblob(self):
        pass

    def _internal_config_apply(
        self, connection: SQCloudConnect, config: SQCloudConfig
    ) -> None:
        if config.timeout > 0:
            connection.socket.settimeout(config.timeout)

        buffer = ""

        if config.account.apikey:
            buffer += f"AUTH APIKEY {connection.account.apikey};"

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

    def _internal_run_command(self, connection: SQCloudConnect, buffer: str) -> None:
        self._internal_socket_write(connection, buffer)
        self._internal_socket_read(connection)

    def _internal_socket_write(self, connection: SQCloudConnect, buffer: str) -> None:
        # compute header
        delimit = "$" if connection.isblob else "+"
        buffer_len = len(buffer)
        header = f"{delimit}{buffer_len} "

        # write header
        try:
            connection.socket.sendall(header.encode())
        except Exception as exc:
            raise SQCloudException(
                "An error occurred while writing header data.",
                SQCLOUD_INTERNAL_ERRCODE.INTERNAL_ERRCODE_NETWORK,
                exc,
            )

        # write buffer
        if buffer_len == 0:
            return
        try:
            connection.socket.sendall(buffer.encode())
        except Exception as exc:
            raise SQCloudException(
                "An error occurred while writing data.",
                SQCLOUD_INTERNAL_ERRCODE.INTERNAL_ERRCODE_NETWORK,
                exc,
            )

    def _internal_socket_read(self, connection: SQCloudConnect) -> any:
        buffer: str = ""
        buffer_size: int = 1024
        nread: int = 0

        try:
            while True:
                data = connection.socket.recv(buffer_size)
                if not data:
                    break
                
                # update buffers
                data = data.decode()
                buffer += data
                nread += len(data)

                c = buffer[0]

                if c == SQCLOUD_CMD.INT or c == SQCLOUD_CMD.FLOAT or c == SQCLOUD_CMD.NULL:
                    if buffer[nread-1] != ' ':
                        continue
                elif c == SQCLOUD_CMD.ROWSET_CHUNK:
                    isEndOfChunk = buffer.endswith(SQCLOUD_CMD.CHUNKS_END)
                    if not isEndOfChunk:
                        continue
                else:
                    n: SQCloudNumber = self._internal_parse_number(buffer)
                    can_be_zerolength = c == SQCLOUD_CMD.BLOB or c == SQCLOUD_CMD.STRING
                    if n.value == 0 and not can_be_zerolength:
                        continue
                    if n.value + n.cstart != nread:
                        continue

                return self._internal_parse_buffer(buffer, nread)

        except Exception as exc:
            raise SQCloudException(
                "An error occurred while reading data from the socket.",
                SQCLOUD_INTERNAL_ERRCODE.INTERNAL_ERRCODE_NETWORK,
                exc,
            )

    def _internal_parse_number(self, buffer: str, index: int = 1) -> SQCloudNumber:
        sqlite_number = SQCloudNumber()
        extvalue = 0
        isext = False
        blen = len(buffer)

        # from 1 to skip the first command type character
        for i in range(index, blen):
            c = buffer[i]

            # check for optional extended error code (ERRCODE:EXTERRCODE)
            if c == ':':
                isext = True
                continue

            # check for end of value
            if c == ' ':
                sqlite_number.cstart = i + 1
                sqlite_number.extcode = extvalue
                return sqlite_number

            # compute numeric value
            if isext:
                extvalue = (extvalue * 10) + int(buffer[i])
            else:
                sqlite_number.value = (sqlite_number.value * 10) + int(buffer[i])

        return 0
    
    def _internal_parse_buffer(self, buffer: str, blen: int) -> any:
        # TODO
        return