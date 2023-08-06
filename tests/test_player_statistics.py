import numpy as np
import pytest
import pandas as pd
from istar.assets.fpl_data.player_stats import PlayerStatistics


def test_add_rolling_means(
    fpl_dataset_fixture, expected_total_points_rolling_previous_5
):
    fpl_dataset = fpl_dataset_fixture
    ps = PlayerStatistics(fpl_dataset=fpl_dataset)
    rolling_total_points = ps._compute_rolling_stats(
        window=5, stats_names=["total_points"], from_future=False
    )
    assert rolling_total_points.shape == (17, 3)
    assert (
        rolling_total_points[rolling_total_points["gameweek"] <= 4][
            "total_points_rolling_previous_5"
        ]
        .isna()
        .all()
    )
    assert (
        not rolling_total_points[rolling_total_points["gameweek"] > 4][
            "total_points_rolling_previous_5"
        ]
        .isna()
        .any()
    )
    pd.testing.assert_frame_equal(
        rolling_total_points, expected_total_points_rolling_previous_5
    )


def test_add_cumulative_features():
    assert 1 == 1
