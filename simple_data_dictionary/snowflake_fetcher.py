import os

# from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

user = os.environ["SNOWFLAKE_USER"]
password = os.environ["SNOWFLAKE_PASSWORD"]
account = os.environ["SNOWFLAKE_ACCOUNT"]
warehouse = os.environ["SNOWFLAKE_WAREHOUSE"]

DATABASES = ["GO", "DEVSANDBOX_RAW_DATA"]


def get_tables(connection: Engine) -> pd.DataFrame:
    dfs = []
    connection.execute(f"USE WAREHOUSE {warehouse};")
    query = (
        "SELECT TABLE_CATALOG, TABLE_SCHEMA, "
        "contact(TABLE_CATALOG,'_', TABLE_SCHEMA) as SCHEMA_ID, "
        "TABLE_NAME, concat(schema_id,'_',TABLE_NAME) as TABLE_ID, "
        "ROW_COUNT, CREATED, LAST_ALTERED "
        "FROM information_schema.TABLES "
        "WHERE TABLE_SCHEMA NOT IN ('PUBLIC', 'INFORMATION_SCHEMA');"
    )
    for db in DATABASES:
        connection.execute(f"USE DATABASE {db};")
        df = pd.read_sql(query, connection)
        dfs.append(df)
    df = pd.concat(dfs, ignore_index=True)
    return df


def db_df(df: pd.DataFrame) -> pd.DataFrame:
    """transforms the query result df into a df for databases"""
    df = df[["table_catalog"]].drop_duplicates("table_catalog")
    df["Id:ID"] = df["table_catalog"]
    df["name"] = df["Id:ID"]
    df[":LABEL"] = "database"
    df.drop(columns=["table_catalog"], inplace=True)
    return df


def schema_df(df: pd.DataFrame) -> pd.DataFrame:
    """transforms the query result into a df for schemas"""
    df = df[["schema_id", "table_schema"]].drop_duplicates("schema_id")
    df["Id:ID"] = df["schema_id"]
    df["name"] = df["table_schema"]
    df[":LABEL"] = "schema"
    df.drop(columns=["schema_id", "table_schema"], inplace=True)
    return df


def table_df(df: pd.DataFrame) -> pd.DataFrame:
    # TODO: add the datetime stuff.
    df = df[["table_catalog", "table_schema", "table_name", "table_id", "row_count"]]
    df["Id:ID"] = df["table_id"]
    df["name"] = df["table_name"]
    df[":LABEL"] = "table"
    df["ROW_COUNT"] = df["row_count"]
    df.drop(
        columns=[
            "table_catalog",
            "table_schema",
            "table_name",
            "table_id",
            "row_count",
        ],
        inplace=True,
    )
    return df


def schema_rels_df(df: pd.DataFrame) -> pd.DataFrame:
    """creates a dataframe of the relationships between schemas and databases"""
    df[":START_ID"] = df["table_catalog"]
    df[":END_ID"] = df["table_schema"]
    df[":TYPE"] = df.apply(lambda row: "BELONGS_TO", axis=1)
    df.drop(
        columns=[
            "table_catalog",
            "table_schema",
            "table_name",
            "row_count",
            "schema_id",
            "table_id",
        ],
        inplace=True,
    )
    df.drop_duplicates(":END_ID", inplace=True)
    return df


def table_rels_df(df: pd.DataFrame) -> pd.DataFrame:
    df[":START_ID"] = df["schema_id"]
    df[":END_ID"] = df["table_id"]
    df[":TYPE"] = df.apply(lambda row: "BELONGS_TO", axis=1)
    df.drop(
        columns=[
            "table_catalog",
            "table_schema",
            "table_name",
            "row_count",
            "schema_id",
            "table_id",
        ],
        inplace=True,
    )
    df.drop_duplicates(":END_ID", inplace=True)
    return df


def main():
    engine = create_engine(f"snowflake://{user}:{password}@{account}/")
    connection = engine.connect()
    df = get_tables(connection)
    schema_rels = schema_rels_df(df)
    print(schema_rels)
