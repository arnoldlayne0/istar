import os

from dagster import Definitions, load_assets_from_package_module
from istar.resources.pandas_io_manager import (
    SimplePandasIOManager,
    local_simple_pandas_io_manager,
)
from istar.assets import fpl_dataset_assets, training_dataset_assets

BASE_PATH = os.getenv("ISTAR_BASE_PATH")
CURRENT_SEASON = "season_22_23"

RESOURCES = {
    "io_manager": local_simple_pandas_io_manager,
    "base_path": f"{BASE_PATH}data/{CURRENT_SEASON}/raw/",
}

all_assets = [
    *fpl_dataset_assets,
    # *training_dataset_assets
]

defs = Definitions(assets=all_assets, resources=RESOURCES)
