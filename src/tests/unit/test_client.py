from sqlitecloud.client import SqliteCloudClient


class TestClient:   
    def test_parse_connection_string_with_apikey(self):
        connection_string = "sqlitecloud://user:pass@host.com:8860/dbname?apikey=abc123&timeout=10&compression=true"
        client = SqliteCloudClient(connection_str=connection_string)

        assert not client.config.account.username
        assert not client.config.account.password
        assert "host.com" == client.config.account.hostname
        assert 8860 == client.config.account.port
        assert "dbname" == client.config.account.dbname
        assert "abc123" == client.config.account.apikey
        assert 10 == client.config.timeout
        assert True == client.config.compression

    def test_parse_connection_string_with_credentials(self):
        connection_string = "sqlitecloud://user:pass@host.com:8860"
        client = SqliteCloudClient(connection_str=connection_string)

        assert "user" == client.config.account.username
        assert "pass" == client.config.account.password
        assert "host.com" == client.config.account.hostname
        assert 8860 == client.config.account.port
        assert not client.config.account.dbname
    
    def test_parse_connection_string_without_credentials(self):
        connection_string = "sqlitecloud://host.com"
        client = SqliteCloudClient(connection_str=connection_string)

        assert not client.config.account.username
        assert not client.config.account.password
        assert "host.com" == client.config.account.hostname

        
    def test_parse_connection_string_with_all_parameters(self):
        connection_string = "sqlitecloud://host.com:8860/dbname?apikey=abc123&compression=true&zerotext=true&memory=true&create=true&non_linearizable=true&insecure=true&no_verify_certificate=true&root_certificate=rootcert&certificate=cert&certificate_key=certkey&noblob=true&maxdata=10&maxrows=11&maxrowset=12"
        
        client = SqliteCloudClient(connection_str=connection_string)
        
        assert "host.com" == client.config.account.hostname
        assert 8860 == client.config.account.port
        assert "dbname" == client.config.account.dbname
        assert "abc123" == client.config.account.apikey
        assert True == client.config.compression
        assert True == client.config.zerotext
        assert True == client.config.memory
        assert True == client.config.create
        assert True == client.config.non_linearizable
        assert True == client.config.insecure
        assert True == client.config.no_verify_certificate
        assert "rootcert" == client.config.root_certificate
        assert "cert" == client.config.certificate
        assert "certkey" == client.config.certificate_key
        assert True == client.config.noblob
        assert 10 == client.config.maxdata
        assert 11 == client.config.maxrows
        assert 12 == client.config.maxrowset