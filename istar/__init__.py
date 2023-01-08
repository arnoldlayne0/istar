from dagster import Definitions, load_assets_from_package_module
from istar.resources.pandas_io_manager import SimplePandasIOManager, local_simple_pandas_io_manager
from istar.assets import fpl_dataset_assets

CURRENT_SEASON = "season_22_23"

RESOURCES = {
    "io_manager": local_simple_pandas_io_manager,
    "base_path": f"/Users/milosztaracha/projects/istar/data/{CURRENT_SEASON}/raw/",
}


defs = Definitions(
    assets=fpl_dataset_assets,
    resources=RESOURCES
)
