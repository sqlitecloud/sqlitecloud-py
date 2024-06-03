from sqlitecloud.client import SQLiteCloudClient


class TestClient:
    def test_parse_connection_string_with_apikey(self):
        connection_string = "sqlitecloud://user:pass@host.com:8860/dbname?apikey=abc123&timeout=10&compression=true"
        client = SQLiteCloudClient(connection_str=connection_string)

        assert not client.config.account.username
        assert not client.config.account.password
        assert "host.com" == client.config.account.hostname
        assert 8860 == client.config.account.port
        assert "dbname" == client.config.account.dbname
        assert "abc123" == client.config.account.apikey
        assert 10 == client.config.timeout
        assert client.config.compression

    def test_parse_connection_string_with_credentials(self):
        connection_string = "sqlitecloud://user:pass@host.com:8860"
        client = SQLiteCloudClient(connection_str=connection_string)

        assert "user" == client.config.account.username
        assert "pass" == client.config.account.password
        assert "host.com" == client.config.account.hostname
        assert 8860 == client.config.account.port
        assert not client.config.account.dbname

    def test_parse_connection_string_without_credentials(self):
        connection_string = "sqlitecloud://host.com"
        client = SQLiteCloudClient(connection_str=connection_string)

        assert not client.config.account.username
        assert not client.config.account.password
        assert "host.com" == client.config.account.hostname

    def test_parse_connection_string_with_all_parameters(self):
        connection_string = "sqlitecloud://host.com:8860/dbname?apikey=abc123&compression=true&zerotext=true&memory=true&create=true&non_linearizable=true&insecure=true&no_verify_certificate=true&root_certificate=rootcert&certificate=cert&certificate_key=certkey&noblob=true&maxdata=10&maxrows=11&maxrowset=12"

        client = SQLiteCloudClient(connection_str=connection_string)

        assert "host.com" == client.config.account.hostname
        assert 8860 == client.config.account.port
        assert "dbname" == client.config.account.dbname
        assert "abc123" == client.config.account.apikey
        assert client.config.compression
        assert client.config.zerotext
        assert client.config.memory
        assert client.config.create
        assert client.config.non_linearizable
        assert client.config.insecure
        assert client.config.no_verify_certificate
        assert "rootcert" == client.config.root_certificate
        assert "cert" == client.config.certificate
        assert "certkey" == client.config.certificate_key
        assert client.config.noblob
        assert 10 == client.config.maxdata
        assert 11 == client.config.maxrows
        assert 12 == client.config.maxrowset
