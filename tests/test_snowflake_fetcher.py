import unittest
import pandas as pd
from simple_data_dictionary import snowflake_fetcher as sf


class TestSnowflakeFetcher(unittest.TestCase):
    def test_db_df(self):
        data = {
            "table_catalog": ["go", "go", "devsandbox_raw_data"],
            "table_schema": ["go", "go", "events"],
            "schema_id": ["go_go", "go_go", "devsandbox_raw_data_events"],
            "table_name": ["blogs", "companies", "events"],
            "row_count": [100, 200, 300],
            # todo: add date shit in here
        }
        input_df = pd.DataFrame(data=data)
        actual = sf.db_df(input_df)
        self.assertEqual(list(actual.columns), ["Id:ID", "name", ":LABEL"])
        self.assertEqual(list(actual["Id:ID"]), ["go", "devsandbox_raw_data"])
        self.assertEqual(actual.shape, (2, 3))

    def test_transform_schema_df(self):
        data = {
            "table_catalog": ["go", "go", "devsandbox_raw_data"],
            "table_schema": ["go", "go", "events"],
            "schema_id": ["go_go", "go_go", "devsandbox_raw_data_events"],
            "table_name": ["blogs", "companies", "events"],
            "row_count": [100, 200, 300],
            # todo: add date shit in here
        }
        input_df = pd.DataFrame(data=data)
        actual = sf.schema_df(input_df)
        self.assertEqual(list(actual.columns), ["Id:ID", "name", ":LABEL"])
        self.assertEqual(list(actual["Id:ID"]), ["go_go", "devsandbox_raw_data_events"])
        self.assertEqual(actual.shape, (2, 3))

    def test_transform_table_df(self):
        # TODO: make this a class variable so it's shared accross all tests
        data = {
            "table_catalog": ["go", "go", "devsandbox_raw_data"],
            "table_schema": ["go", "go", "events"],
            "schema_id": ["go_go", "go_go", "devsandbox_raw_data_events"],
            "table_name": ["blogs", "companies", "events"],
            "table_id": [
                "GO_GO_BLOGS",
                "GO_GO_COMPANIES",
                "DEVSANDBOX_RAW_DATA_EVENTS_EVENTS",
            ],
            "row_count": [100, 200, 300],
            # todo: add date shit in here
        }
        input_df = pd.DataFrame(data=data)
        actual = sf.table_df(input_df)
        self.assertEqual(list(actual.columns), ["Id:ID", "name", ":LABEL", "ROW_COUNT"])
        self.assertEqual(
            list(actual["Id:ID"]),
            ["GO_GO_BLOGS", "GO_GO_COMPANIES", "DEVSANDBOX_RAW_DATA_EVENTS_EVENTS"],
        )
        self.assertEqual(actual.shape, (3, 4))

    def test_schema_rels_df(self):
        # TODO: make this a class variable
        data = {
            "table_catalog": ["go", "go", "devsandbox_raw_data"],
            "table_schema": ["go", "go", "events"],
            "schema_id": ["go_go", "go_go", "devsandbox_raw_data_events"],
            "table_name": ["blogs", "companies", "events"],
            "table_id": [
                "GO_GO_BLOGS",
                "GO_GO_COMPANIES",
                "DEVSANDBOX_RAW_DATA_EVENTS_EVENTS",
            ],
            "row_count": [100, 200, 300],
            # todo: add date shit in here
        }
        input_df = pd.DataFrame(data=data)
        actual = sf.schema_rels_df(input_df)
        self.assertEqual(list(actual.columns), [":START_ID", ":END_ID", ":TYPE"])
        self.assertEqual(list(actual[":START_ID"]), ["go", "devsandbox_raw_data"])

    def test_table_rels_df(self):
        # TODO: make this a class variable
        data = {
            "table_catalog": ["go", "go", "devsandbox_raw_data"],
            "table_schema": ["go", "go", "events"],
            "schema_id": ["go_go", "go_go", "devsandbox_raw_data_events"],
            "table_name": ["blogs", "companies", "events"],
            "table_id": [
                "GO_GO_BLOGS",
                "GO_GO_COMPANIES",
                "DEVSANDBOX_RAW_DATA_EVENTS_EVENTS",
            ],
            "row_count": [100, 200, 300],
            # todo: add date shit in here
        }
        input_df = pd.DataFrame(data=data)
        actual = sf.table_rels_df(input_df)
        self.assertEqual(list(actual.columns), [":START_ID", ":END_ID", ":TYPE"])
        self.assertEqual(
            list(actual[":START_ID"]), ["go_go", "go_go", "devsandbox_raw_data_events"]
        )
