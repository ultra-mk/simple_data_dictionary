import csv
from dataclasses import dataclass, field
import os
from pathlib import Path
from typing import List
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

user = os.environ["SNOWFLAKE_USER"]
password = os.environ["SNOWFLAKE_PASSWORD"]
account = os.environ["SNOWFLAKE_ACCOUNT"]
warehouse = os.environ["SNOWFLAKE_WAREHOUSE"]

DATABASES = ["GO", "DEVSANDBOX_RAW_DATA"]


@dataclass
class Schema:
    database: str
    schema: str
    schema_id: str = field(init=False)

    def __post_init__(self):
        self.schema_id = f"{self.database.lower()}_{self.schema.lower()}"


def get_schemas(connection: Engine) -> List[Schema]:
    """returns a list of databases"""
    schemas = []
    connection.execute(f"USE WAREHOUSE {warehouse};")
    for db in DATABASES:
        connection.execute(f"USE DATABASE {db};")
        query_result = connection.execute(
            "SELECT CATALOG_NAME, SCHEMA_NAME "
            "FROM INFORMATION_SCHEMA.SCHEMATA "
            "WHERE SCHEMA_NAME NOT IN ('PUBLIC', 'INFORMATION_SCHEMA');"
        ).fetchall()
        for row in query_result:
            schemas.append(Schema(*row))
    return schemas


def db_nodes_to_csv(databases: List[str], csv_dir: Path) -> None:
    """writes the database nodes to csv"""
    header = ["dbId:ID", "db", ":Label"]
    with open(f"{csv_dir}/db_nodes.csv", "w") as outfile:
        writer = csv.writer(outfile)
        writer.writerow(header)
        for db in databases:
            writer.writerow([db.lower(), db, "database"])


def main():
    engine = create_engine(f"snowflake://{user}:{password}@{account}/")
    connection = engine.connect()
    csv_dir = Path(f"{Path.cwd()}/csvs")
    schemas = get_schemas(connection)
    print(schemas)
    db_nodes_to_csv(DATABASES, csv_dir)
