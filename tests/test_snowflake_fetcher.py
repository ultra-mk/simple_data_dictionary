import unittest
import pandas as pd
from simple_data_dictionary import snowflake_fetcher as sf


class TestSnowflakeFetcher(unittest.TestCase):
    def test_db_df(self):
        data = {
            "table_catalog": ["GO", "GO", "DEVSANDBOX_RAW_DATA"],
            "table_schema": ["GO", "GO", "EVENTS"],
            "table_name": ["BLOGS", "COMPANIES", "EVENTS"],
            "row_count": [100, 200, 300],
            # TODO: add date shit in here
        }
        input_df = pd.DataFrame(data=data)
        actual = sf.db_df(input_df)
        self.assertEqual(list(actual.columns), ["Id:ID", "name", ":LABEL"])
        self.assertEqual(list(actual["Id:ID"]), ["GO", "DEVSANDBOX_RAW_DATA"])
        self.assertEqual(actual.shape, (2, 3))

    def test_tansform_schema_df(self):
        data = {
            "table_catalog": ["GO", "GO", "DEVSANDBOX_RAW_DATA"],
            "table_schema": ["GO", "GO", "EVENTS"],
            "table_name": ["BLOGS", "COMPANIES", "EVENTS"],
            "row_count": [100, 200, 300],
        }
        input_df = pd.DataFrame(data=data)
        actual = sf.schema_df(input_df)
        self.assertEqual(list(actual.columns), ["Id:ID", "name", ":LABEL"])
        self.assertEqual(list(actual["Id:ID"]), ["GO_GO", "DEVSANDBOX_RAW_DATA_EVENTS"])
        self.assertEqual(actual.shape, (2, 3))
