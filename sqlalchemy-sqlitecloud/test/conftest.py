import pytest
from sqlalchemy.dialects import registry

pytest.register_assert_rewrite("sqlalchemy.testing.assertions")

registry.register("sqlitecloud", "sqlalchemy_sqlitecloud.base", "SQLiteCloudDialect")


from sqlalchemy.testing.plugin.pytestplugin import *  # noqa
