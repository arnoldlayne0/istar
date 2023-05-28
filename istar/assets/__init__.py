from dagster import load_assets_from_package_module
from . import fpl_data

FPL_DATASETS = "fpl_datasets"
TRAINING_DATASETS = "training_datasets"

fpl_dataset_assets = load_assets_from_package_module(
    package_module=fpl_data, group_name=FPL_DATASETS
)
training_dataset_assets = load_assets_from_package_module(
    package_module=fpl_data, group_name=TRAINING_DATASETS
)