import os
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
        "concat(TABLE_CATALOG,'_',TABLE_SCHEMA) as SCHEMA_ID, "
        "TABLE_NAME, concat(SCHEMA_ID,'_', TABLE_NAME) as TABLE_ID, "
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


def db_schema_table_nodes_to_csv(df: pd.DataFrame) -> None:
    """writes the db, schema and table nodes to csv"""
    csvs_path = str(f"{Path.cwd()}/csvs")
    # glue these headings to the dfs
    # db_header = ["dbId:ID", "db", ":LABEL"]
    # schema_header = ["schemaId:ID", "schema", ":LABEL"]
    # TODO: add in the datetime stuff too
    # table_header = ["tableId:ID", "table", ":LABEL", "row_count:int"]
    dbs = df[["table_catalog"]].drop_duplicates("table_catalog")
    schemas = df[["table_schema", "schema_id"]].drop_duplicates("schema_id")
    tables = df[["table_name", "table_id", "row_count"]].drop_duplicates("table_id")
    # TODO: have it return a list of dfs.
    dbs.to_csv(f"{csvs_path}/db.csv", index=False)
    schemas.to_csv(f"{csvs_path}/schemas.csv", index=False)
    tables.to_csv(f"{csvs_path}/tables.csv", index=False)


def db_schema_tables_rels_to_csv(df: pd.DataFrame) -> None:
    """writes the relationships to csv"""
    csvs_path = str(f"{Path.cwd()}/csvs")
    # rel_header = [":START_ID", ":END_ID", ":TYPE"]
    df["rel_type"] = df.apply(lambda row: "BELONGS_TO", axis=1)
    dbs_schema = df[["table_catalog", "schema_id", "rel_type"]].drop_duplicates()
    schemas_tables = df[["schema_id", "table_id", "rel_type"]].drop_duplicates()
    dbs_schema.to_csv(f"{csvs_path}/rels/dbs_schemas.csv")
    schemas_tables.to_csv(f"{csvs_path}/rels/schemas_tables.csv")


def main():
    engine = create_engine(f"snowflake://{user}:{password}@{account}/")
    connection = engine.connect()
    df = get_tables(connection)
    print(db_schema_table_nodes_to_csv(df))
    print(db_schema_tables_rels_to_csv(df))
