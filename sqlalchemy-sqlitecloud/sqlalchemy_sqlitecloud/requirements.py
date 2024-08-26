from sqlalchemy.testing import exclusions
from sqlalchemy.testing.requirements import SuiteRequirements

supported = exclusions.open
unsupported = exclusions.closed


class Requirements(SuiteRequirements):
    @property
    def table_ddl_if_exists(self):
        """target platform supports IF NOT EXISTS / IF EXISTS for tables."""

        return supported()

    @property
    def index_ddl_if_exists(self):
        """target platform supports IF NOT EXISTS / IF EXISTS for indexes."""

        return supported()

    @property
    def implicitly_named_constraints(self):
        """target database must apply names to unnamed constraints."""

        return unsupported()

    @property
    def reflects_pk_names(self):
        """https://www.sqlite.org/syntax/column-constraint.html"""

        return supported()

    @property
    def views(self):
        """Target database must support VIEWs."""

        return supported()

    @property
    def dbapi_lastrowid(self):
        """target platform includes a 'lastrowid' accessor on the DBAPI
        cursor object.

        """
        return supported()
