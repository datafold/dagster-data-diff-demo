from dagster import Definitions, load_assets_from_modules

from . import assets, simple_demo

all_assets = load_assets_from_modules([assets, simple_demo])

defs = Definitions(
    assets=all_assets,
    asset_checks=[simple_demo.data_diff_check]
)
