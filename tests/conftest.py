import pytest
import numpy as np
import pandas as pd


@pytest.fixture()
def fpl_dataset_fixture():
    return pd.read_csv("tests/data/fpl_dataset_sample.csv")


@pytest.fixture()
def expected_total_points_rolling_previous_5():
    return pd.read_csv("tests/data/expected_total_points_rolling_previous_5.csv")
