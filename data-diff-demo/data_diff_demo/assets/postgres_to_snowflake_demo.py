import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.dialects import registry
from dotenv import load_dotenv
import os

from dagster import (
    asset,
    asset_check,
    Definitions,
    AssetCheckResult,
    MetadataValue,
    AssetCheckSeverity,
)
from data_diff import connect_to_table, diff_tables
from random import randint

load_dotenv()

SOURCE_DATABASE_HOST = os.getenv("SOURCE_DATABASE_HOST")
SOURCE_DATABASE_PORT = os.getenv("SOURCE_DATABASE_PORT")
SOURCE_DATABASE_NAME = os.getenv("SOURCE_DATABASE_NAME")
SOURCE_DATABASE_USER = os.getenv("SOURCE_DATABASE_USER")
SOURCE_DATABASE_PASSWORD = os.getenv("SOURCE_DATABASE_PASSWORD")
DESTINATION_SNOWFLAKE_ACCOUNT = os.getenv("DESTINATION_SNOWFLAKE_ACCOUNT")
DESTINATION_SNOWFLAKE_USER = os.getenv("DESTINATION_SNOWFLAKE_USER")
DESTINATION_SNOWFLAKE_PASSWORD = os.getenv("DESTINATION_SNOWFLAKE_PASSWORD")
DESTINATION_SNOWFLAKE_WAREHOUSE = os.getenv("DESTINATION_SNOWFLAKE_WAREHOUSE")
DESTINATION_SNOWFLAKE_DATABASE = os.getenv("DESTINATION_SNOWFLAKE_DATABASE")
DESTINATION_SNOWFLAKE_SCHEMA = os.getenv("DESTINATION_SNOWFLAKE_SCHEMA")
DESTINATION_SNOWFLAKE_ROLE = os.getenv("DESTINATION_SNOWFLAKE_ROLE")

EVENT_COUNT = 100


@asset
def source_postgres_events():
    conn = psycopg2.connect(
        host=SOURCE_DATABASE_HOST,
        port=SOURCE_DATABASE_PORT,
        dbname=SOURCE_DATABASE_NAME,
        user=SOURCE_DATABASE_USER,
        password=SOURCE_DATABASE_PASSWORD,
    )
    cursor = conn.cursor()

    cursor.execute("DROP TABLE IF EXISTS events;")

    create_table_query = """
        CREATE TABLE events (
            "ID" INTEGER,
            "DATE" DATE
        );
    """
    cursor.execute(create_table_query)

    for i in range(EVENT_COUNT):
        cursor.execute(f"INSERT INTO events VALUES ({i}, current_date + {i});")

    noise_size = randint(1, 5)
    for i in range(noise_size):
        cursor.execute(f"INSERT INTO events VALUES (-{i}, current_date - {i});")

    conn.commit()

    cursor.close()
    conn.close()


@asset(deps=[source_postgres_events])
def replicated_snowflake_events():
    src_conn = psycopg2.connect(
        host=SOURCE_DATABASE_HOST,
        port=SOURCE_DATABASE_PORT,
        dbname=SOURCE_DATABASE_NAME,
        user=SOURCE_DATABASE_USER,
        password=SOURCE_DATABASE_PASSWORD,
    )
    src_df = pd.read_sql('SELECT * FROM events WHERE "ID" > 0', src_conn)
    src_conn.close()

    registry.register("snowflake", "snowflake.sqlalchemy", "dialect")

    sf_engine = create_engine(
        f"snowflake://{DESTINATION_SNOWFLAKE_USER}:{DESTINATION_SNOWFLAKE_PASSWORD}@{DESTINATION_SNOWFLAKE_ACCOUNT}/{DESTINATION_SNOWFLAKE_DATABASE}/{DESTINATION_SNOWFLAKE_SCHEMA}?warehouse={DESTINATION_SNOWFLAKE_WAREHOUSE}&role={DESTINATION_SNOWFLAKE_ROLE}"
    )

    src_df.to_sql("events", sf_engine, if_exists="replace", index=False)

    with sf_engine.connect() as sf_conn:
        noise_size = randint(5, 15)
        for i in range(noise_size):
            sf_conn.execute(
                f"UPDATE events SET DATE = CURRENT_DATE() - 2 WHERE ID = {i};"
            )


@asset_check(
    name="postgres_to_snowflake_data_diff_check",
    asset=replicated_snowflake_events,
)
def postgres_to_snowflake_data_diff_check() -> AssetCheckResult:
    template_postgres = {
        "driver": "postgresql",
    }

    template_snowflake = {
        "driver": "snowflake",
    }

    source_events_table = connect_to_table(
        {
            **template_postgres,
            "host": SOURCE_DATABASE_HOST,
            "port": SOURCE_DATABASE_PORT,
            "dbname": SOURCE_DATABASE_NAME,
            "user": SOURCE_DATABASE_USER,
            "password": SOURCE_DATABASE_PASSWORD,
        },
        "events",
    )

    replicated_events_table = connect_to_table(
        {
            **template_snowflake,
            "account": DESTINATION_SNOWFLAKE_ACCOUNT,
            "user": DESTINATION_SNOWFLAKE_USER,
            "password": DESTINATION_SNOWFLAKE_PASSWORD,
            "warehouse": DESTINATION_SNOWFLAKE_WAREHOUSE,
            "database": DESTINATION_SNOWFLAKE_DATABASE,
            "schema": DESTINATION_SNOWFLAKE_SCHEMA,
            "role": DESTINATION_SNOWFLAKE_ROLE,
        },
        "EVENTS",
    )

    results = pd.DataFrame(
        diff_tables(
            source_events_table,
            replicated_events_table,
            key_columns=["ID"],
            extra_columns=("DATE",),
        ),
        columns=["diff_type", "row_diffs"],
    )

    total_diffs_count = len(results)

    yield AssetCheckResult(
        passed=total_diffs_count == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "total_diffs": MetadataValue.int(total_diffs_count),
            "source_row_diffs": MetadataValue.int(
                len(results[results["diff_type"] == "-"])
            ),
            "target_row_diffs": MetadataValue.int(
                len(results[results["diff_type"] == "+"])
            ),
            "preview": MetadataValue.md(results.head(100).to_markdown()),
        },
    )


defs = Definitions(
    assets=[source_postgres_events, replicated_snowflake_events],
    asset_checks=[postgres_to_snowflake_data_diff_check],
)
