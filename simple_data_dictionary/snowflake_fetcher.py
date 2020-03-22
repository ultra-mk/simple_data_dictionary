import os
from typing import List

from pathlib import Path
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
        "concat(TABLE_CATALOG,'_', TABLE_SCHEMA) as SCHEMA_ID, "
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
    local_df = df.copy(deep=True)
    local_df = local_df[["table_catalog"]].drop_duplicates("table_catalog")
    local_df["Id:ID"] = local_df["table_catalog"]
    local_df["name"] = local_df["Id:ID"]
    local_df[":LABEL"] = "database"
    local_df.drop(columns=["table_catalog"], inplace=True)
    return local_df


def schema_df(df: pd.DataFrame) -> pd.DataFrame:
    """transforms the query result into a df for schemas"""
    local_df = df.copy(deep=True)
    local_df = local_df[["schema_id", "table_schema"]].drop_duplicates("schema_id")
    local_df["Id:ID"] = local_df["schema_id"]
    local_df["name"] = local_df["table_schema"]
    local_df[":LABEL"] = "schema"
    local_df.drop(columns=["schema_id", "table_schema"], inplace=True)
    return local_df


def table_df(df: pd.DataFrame) -> pd.DataFrame:
    # TODO: add the datetime stuff.
    local_df = df.copy(deep=True)
    local_df = local_df[
        ["table_catalog", "table_schema", "table_name", "table_id", "row_count"]
    ]
    local_df["Id:ID"] = local_df["table_id"]
    local_df["name"] = local_df["table_name"]
    local_df[":LABEL"] = "table"
    local_df["ROW_COUNT"] = local_df["row_count"]
    local_df.drop(
        columns=[
            "table_catalog",
            "table_schema",
            "table_name",
            "table_id",
            "row_count",
        ],
        inplace=True,
    )
    return local_df


def schema_rels_df(df: pd.DataFrame) -> pd.DataFrame:
    """creates a dataframe of the relationships between schemas and databases"""
    local_df = df.copy(deep=True)
    local_df[":START_ID"] = local_df["table_catalog"]
    local_df[":END_ID"] = local_df["table_schema"]
    local_df[":TYPE"] = local_df.apply(lambda row: "BELONGS_TO", axis=1)
    local_df.drop(
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
    local_df.drop_duplicates(":END_ID", inplace=True)
    return local_df


def table_rels_df(df: pd.DataFrame) -> pd.DataFrame:
    local_df = df.copy(deep=True)
    local_df[":START_ID"] = local_df["schema_id"]
    local_df[":END_ID"] = local_df["table_id"]
    local_df[":TYPE"] = local_df.apply(lambda row: "BELONGS_TO", axis=1)
    local_df.drop(
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
    local_df.drop_duplicates(":END_ID", inplace=True)
    return local_df


def write_dfs_to_csv(dfs: List[pd.DataFrame]) -> None:
    # TODO: write a check on the length of the dataframe
    file_names = [
        "dbs.csv",
        "schemas.csv",
        "tables.csv",
        "schema_rels.csv",
        "table_rels.csv",
    ]
    csv_path = Path(f"{Path.cwd()}/csvs")
    for ix, df in enumerate(dfs):
        df.to_csv(path_or_buf=Path(f"{csv_path}/{file_names[ix]}"), index=False)


def main():
    engine = create_engine(f"snowflake://{user}:{password}@{account}/")
    connection = engine.connect()
    dfs = []
    df = get_tables(connection)
    dfs.append(db_df(df))
    dfs.append(schema_df(df))
    dfs.append(table_df(df))
    dfs.append(schema_rels_df(df))
    dfs.append(table_rels_df(df))
    write_dfs_to_csv(dfs)
