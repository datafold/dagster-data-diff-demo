import pandas as pd
import psycopg2
import snowflake.connector

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

SOURCE_DATABASE_HOST = "localhost"
SOURCE_DATABASE_PORT = "5432"
SOURCE_DATABASE_NAME = "source_db"
SOURCE_DATABASE_USER = "postgres"
SOURCE_DATABASE_PASSWORD = "password"
DESTINATION_SNOWFLAKE_ACCOUNT = "BYA42734"
DESTINATION_SNOWFLAKE_USER = "<user>"
DESTINATION_SNOWFLAKE_PASSWORD = "<password>"
DESTINATION_SNOWFLAKE_WAREHOUSE = "<warehouse>"
DESTINATION_SNOWFLAKE_DATABASE = "<database>"
DESTINATION_SNOWFLAKE_SCHEMA = "<schema>"
EVENT_COUNT = 100


@asset
def source_events():
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
            id INTEGER,
            date DATE
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


@asset(deps=[source_events])
def replicated_events():
    src_conn = psycopg2.connect(
        host=SOURCE_DATABASE_HOST,
        port=SOURCE_DATABASE_PORT,
        dbname=SOURCE_DATABASE_NAME,
        user=SOURCE_DATABASE_USER,
        password=SOURCE_DATABASE_PASSWORD,
    )
    src_df = pd.read_sql("SELECT * FROM events WHERE id >= 0", src_conn)
    src_conn.close()

    sf_conn = snowflake.connector.connect(
        account=DESTINATION_SNOWFLAKE_ACCOUNT,
        user=DESTINATION_SNOWFLAKE_USER,
        password=DESTINATION_SNOWFLAKE_PASSWORD,
        warehouse=DESTINATION_SNOWFLAKE_WAREHOUSE,
        database=DESTINATION_SNOWFLAKE_DATABASE,
        schema=DESTINATION_SNOWFLAKE_SCHEMA,
    )
    src_df.to_sql("events", sf_conn, if_exists="replace", index=False)
    sf_cursor = sf_conn.cursor()

    noise_size = randint(5, 15)
    for i in range(noise_size):
        sf_cursor.execute(
            f"UPDATE events SET date = CURRENT_DATE() - INTERVAL '{i} days' WHERE id = {i};"
        )

    sf_conn.commit()
    sf_cursor.close()
    sf_conn.close()


@asset_check(
    name="data_diff_check",
    asset=replicated_events,
)
def data_diff_check() -> AssetCheckResult:
    template = {
        "postgres_driver": "psycopg2",
        "snowflake_driver": "snowflake.connector",
    }

    source_events_table = connect_to_table(
        {
            **template,
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
            **template,
            "account": DESTINATION_SNOWFLAKE_ACCOUNT,
            "user": DESTINATION_SNOWFLAKE_USER,
            "password": DESTINATION_SNOWFLAKE_PASSWORD,
            "warehouse": DESTINATION_SNOWFLAKE_WAREHOUSE,
            "database": DESTINATION_SNOWFLAKE_DATABASE,
            "schema": DESTINATION_SNOWFLAKE_SCHEMA,
        },
        "events",
    )

    results = pd.DataFrame(
        diff_tables(
            source_events_table,
            replicated_events_table,
            key_columns=["id"],
            extra_columns=("date",),
        ),
        columns=["diff_type", "row_diffs"],
    )

    total_diffs_count = len(results)

    yield AssetCheckResult(
        passed=total_diffs_count <= 50,
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
    assets=[source_events, replicated_events],
    asset_checks=[data_diff_check],
)
