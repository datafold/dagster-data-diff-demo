from dagster import asset, asset_check, Definitions, AssetCheckResult, MetadataValue, AssetCheckSeverity
from data_diff import connect_to_table, diff_tables
import warnings
warnings.simplefilter("ignore") # TODO: remove this to see all experimental warnings
from random import randint

import duckdb
import pandas as pd

SOURCE_DATABASE_PATH = "data/source_healing.db"
AUDIT_STORAGE_PATH = "data/staging_healing.parquet"
DESTINATION_DATABASE_PATH = "data/destination_healing.db"

EVENT_COUNT = 100

@asset
def source_healing_events():
    query = "create or replace table events as ("

    for i in range(EVENT_COUNT):
        query += f" select {i} as id, current_date + {i} as date union all "
    
    noise_size = randint(1, 5)

    for i in range(noise_size):
        query += f" select - {i} as id, current_date - {i} as date"
        if i < noise_size - 1:
            query += " union all "

    query += ");"

    conn = duckdb.connect(SOURCE_DATABASE_PATH)
    conn.query(query)

@asset(
    deps=[source_healing_events]
)
def replicated_healing_events():
    src_conn = duckdb.connect(SOURCE_DATABASE_PATH)

    dump_to_parquet_query = f"""
        copy (
            select *
            from events
        ) to '{AUDIT_STORAGE_PATH}' (
            format 'parquet'
        );
    """

    src_conn.query(dump_to_parquet_query)

    dest_conn = duckdb.connect(DESTINATION_DATABASE_PATH)

    load_into_destination_query = f"""
        create or replace table events as (
            select * from '{AUDIT_STORAGE_PATH}'
            where id >= 0);
    """

    dest_conn.query(load_into_destination_query)

    noise_size = randint(5, 15)

    for i in range(noise_size):
        update_rows_query = f"""
        UPDATE events
                SET date = current_date - {i}
                WHERE id = {i};
        """
        dest_conn.query(update_rows_query)


@asset_check(
    name="data_diff_healing_check",
    asset=replicated_healing_events,
)
def data_diff_healing_check() -> AssetCheckResult:
    template = {
        'driver': 'duckdb'
    }

    source_events_table = connect_to_table({
        **template,
        "filepath": SOURCE_DATABASE_PATH
    }, "main.events")

    replicated_events_table = connect_to_table({
        **template,
        "filepath": DESTINATION_DATABASE_PATH
    }, "main.events")

    results = pd.DataFrame(diff_tables(source_events_table, replicated_events_table, key_columns=["id"], extra_columns=("date",)), columns=["diff_type", "row_diffs"])

    total_diffs_count = len(results)

    # This is where the healing begins
    source_row_diffs = results[results["diff_type"] == "-"]

    con = duckdb.connect(DESTINATION_DATABASE_PATH)

    for idx, row in source_row_diffs.iterrows():
        row_id = int(row["row_diffs"][0])  # Ensure `id` is an integer.
        row_date = row["row_diffs"][1]

        query = f'''
        UPDATE events
        SET date = '{row_date}'
        WHERE id = {row_id}
        '''

        con.execute(query)

    healed_diff_results = pd.DataFrame(diff_tables(source_events_table, replicated_events_table, key_columns=["id"], extra_columns=("date",)), columns=["diff_type", "row_diffs"])

    total_healed_diffs_count = len(healed_diff_results)

    yield AssetCheckResult(
        passed=total_healed_diffs_count <= 5,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "total_diffs_unhealed": MetadataValue.int(total_healed_diffs_count),
            "source_row_diffs": MetadataValue.int(len(results[results["diff_type"] == "-"])),
            "target_row_diffs": MetadataValue.int(len(results[results["diff_type"] == "+"])),
            "preview_all_diffs": MetadataValue.md(results.head(100).to_markdown()),
            "preview_diff_overwrites": MetadataValue.md(source_row_diffs.head(100).to_markdown()),
            "preview_diffs_remaining": MetadataValue.md(healed_diff_results.head(100).to_markdown())
        }
    )

defs = Definitions(
    assets=[source_healing_events, replicated_healing_events],
    asset_checks=[data_diff_healing_check],
)