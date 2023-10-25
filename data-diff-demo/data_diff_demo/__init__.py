from dagster import Definitions, load_assets_from_modules

from . import simple_diff_demo, healing_diff_demo, postgres_to_snowflake_demo

all_assets = load_assets_from_modules(
    [simple_diff_demo, healing_diff_demo, postgres_to_snowflake_demo]
)

defs = Definitions(
    assets=all_assets,
    asset_checks=[
        simple_diff_demo.data_diff_check,
        healing_diff_demo.data_diff_healing_check,
        postgres_to_snowflake_demo.postgres_to_snowflake_data_diff_check,
    ],
)
