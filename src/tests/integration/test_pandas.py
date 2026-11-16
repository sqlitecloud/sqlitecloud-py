import os

import pandas as pd
from pandas.testing import assert_frame_equal


# Integration tests for sqlitecloud and pandas dataframe
class TestPanads:
    def test_insert_from_dataframe(self, sqlitecloud_dbapi2_connection):
        conn = sqlitecloud_dbapi2_connection

        dfprices = pd.read_csv(
            os.path.join(os.path.dirname(__file__), "../assets/prices.csv")
        )

        dfmapping = pd.DataFrame(
            {
                "AXP": ["American Express Company"],
                "GE": ["General Electric Company"],
                "GS": ["Goldman Sachs Group Inc"],
                "UTX": ["United Technologies Corporation"],
            }
        )

        for table in ["PRICES", "TICKER_MAPPING"]:
            conn.execute(f"DROP TABLE IF EXISTS {table}")

        # arg if_exists="replace" raises the error
        dfprices.to_sql("PRICES", conn, index=False)
        dfmapping.to_sql("TICKER_MAPPING", conn, index=False)

        df_actual_tables = pd.read_sql(
            "SELECT name FROM sqlite_master WHERE type='table'", conn
        )
        df_actual_prices = pd.read_sql("SELECT * FROM PRICES", conn)
        df_actual_mapping = pd.read_sql("SELECT * FROM TICKER_MAPPING", conn)

        assert "PRICES" in df_actual_tables["name"].to_list()
        assert "TICKER_MAPPING" in df_actual_tables["name"].to_list()
        assert_frame_equal(
            df_actual_prices,
            dfprices,
            check_exact=False,
            atol=1e-6,
            check_dtype=False,
        )
        assert_frame_equal(
            df_actual_mapping,
            dfmapping,
            check_exact=False,
            atol=1e-6,
            check_dtype=False,
        )

    def test_select_into_dataframe(self, sqlitecloud_dbapi2_connection):
        conn = sqlitecloud_dbapi2_connection

        query = "SELECT * FROM albums"
        df = pd.read_sql_query(query, conn)
        cursor = conn.execute(query)

        assert df.columns.to_list() == [
            description[0] for description in cursor.description
        ]
        # compare as tuples
        assert list(df.itertuples(index=False, name=None)) == cursor.fetchall()
