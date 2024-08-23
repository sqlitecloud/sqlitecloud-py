from sqlalchemy.dialects import registry

registry.register(
    "sqlite.sqlitecloud", "sqlalchemy_sqlitecloud.base", "SQLiteCloudDialect"
)
