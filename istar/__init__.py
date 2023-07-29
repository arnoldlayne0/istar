import os

from dagster import Definitions, load_assets_from_package_module
from istar.resources.pandas_io_manager import local_simple_pandas_io_manager
from istar.resources.joblib_io_manager import local_simple_joblib_io_manager
from istar.assets import fpl_dataset_assets

BASE_PATH = os.getenv("ISTAR_BASE_PATH")
CURRENT_SEASON = "season_23_24"

RESOURCES = {
    "pandas_local_io_manager": local_simple_pandas_io_manager,
    "base_path": f"{BASE_PATH}data/{CURRENT_SEASON}/raw/",
    "joblib_local_io_manager": local_simple_joblib_io_manager,
}

all_assets = [*fpl_dataset_assets]

defs = Definitions(assets=all_assets, resources=RESOURCES)
