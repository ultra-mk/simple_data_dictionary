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


# TODO: make this just a simple extraction. Add the transform logic in the next method
def get_tables(connection: Engine) -> pd.DataFrame:
    dfs = []
    connection.execute(f"USE WAREHOUSE {warehouse};")
    query = (
        "SELECT TABLE_CATALOG, TABLE_SCHEMA, "
        "TABLE_NAME, "
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
    df = df[["table_catalog", "table_schema"]].drop_duplicates("table_schema")
    # TODO: make these fs trings
    df["Id:ID"] = df.apply(
        lambda row: row["table_catalog"] + "_" + row["table_schema"], axis=1
    )
    df["name"] = df["table_schema"]
    df[":LABEL"] = "schema"
    df.drop(columns=["table_catalog", "table_schema"], inplace=True)
    return df


def table_df(df: pd.DataFrame) -> pd.DataFrame:
    # TODO: add the datetime stuff.
    df = df[["table_catalog", "table_schema", "table_name", "row_count"]]
    df["Id:ID"] = df.apply(
        # TODO: make these fstrings
        lambda row: row["table_catalog"]
        + "_"
        + row["table_schema"]
        + "_"
        + row["table_name"],
        axis=1,
    )
    df["name"] = df["table_name"]
    df[":LABEL"] = "table"
    df["ROW_COUNT"] = df["row_count"]
    df.drop(
        columns=["table_catalog", "table_schema", "table_name", "row_count"],
        inplace=True,
    )
    return df


def rels_df(df: pd.DataFrame) -> pd.DataFrame:
    """creates a dataframe of the relationships"""
    pass
    # csvs_path = str(f"{Path.cwd()}/csvs")
    # rel_header = [":START_ID", ":END_ID", ":TYPE"]
    # df["rel_type"] = df.apply(lambda row: "BELONGS_TO", axis=1)
    # dbs_schema = df[["table_catalog", "schema_id", "rel_type"]].drop_duplicates()
    # schemas_tables = df[["schema_id", "table_id", "rel_type"]].drop_duplicates()
    # return None
    # dbs_schema.to_csv(f"{csvs_path}/rels/dbs_schemas.csv")

    # schemas_tables.to_csv(f"{csvs_path}/rels/schemas_tables.csv")


def main():
    engine = create_engine(f"snowflake://{user}:{password}@{account}/")
    connection = engine.connect()
    df = get_tables(connection)
    print(table_df(df))
