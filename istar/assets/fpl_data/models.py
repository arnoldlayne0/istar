from typing import Tuple

import pandas as pd
from sklearn.compose import make_column_selector, make_column_transformer
from sklearn.pipeline import Pipeline, make_pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from xgboost import XGBRegressor

from istar.assets.fpl_data.training_data import LabelEnum, STATS_WINDOW, PlayerStatsEnum

CATEGORICAL_FEATURES = [
    "opponent_team",
    "was_home",
    "element_type",
    "team_id",
]

NUMERICAL_FEATURES = [
    # "feature_{stats_window}"
    f"bps_rolling_previous_{STATS_WINDOW}",
    f"creativity_rolling_previous_{STATS_WINDOW}",
    f"expected_goal_involvements_rolling_previous_{STATS_WINDOW}",
    f"expected_goals_conceded_rolling_previous_{STATS_WINDOW}",
    f"influence_rolling_previous_{STATS_WINDOW}",
    f"selected_by_percent_rolling_previous_{STATS_WINDOW}",
    f"threat_rolling_previous_{STATS_WINDOW}",
    f"total_points_rolling_previous_{STATS_WINDOW}",
    f"transfers_balance_rolling_previous_{STATS_WINDOW}",
    f"value_rolling_previous_{STATS_WINDOW}",
    "total_points_against_team_element",
    "total_points_by_team_element",
]


class BaseModel:
    def __init__(self, data):
        self.data = data


class SklearnModel:
    def __init__(self, data: pd.DataFrame, label: LabelEnum = LabelEnum.total_points_current):
        self.data = data
        self.label = label

    def _split_data(
        self,
    ) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        data = self.data.copy()
        train, test = data[data["set"] == "train"], data[data["set"] == "test"]

        X_train, y_train = (
            train[CATEGORICAL_FEATURES + NUMERICAL_FEATURES],
            train["total_points"],
        )
        X_train[CATEGORICAL_FEATURES] = X_train[CATEGORICAL_FEATURES].astype("category")

        X_test, y_test = (
            test[CATEGORICAL_FEATURES + NUMERICAL_FEATURES],
            test["total_points"],
        )
        X_test[CATEGORICAL_FEATURES] = X_test[CATEGORICAL_FEATURES].astype("category")

        return X_train, y_train, X_test, y_test

    def _get_model_pipeline(self) -> Pipeline:
        one_hot_encoder = make_column_transformer(
            (
                OneHotEncoder(sparse=False),
                make_column_selector(dtype_include="category"),
            ),
            (
                StandardScaler(),
                make_column_selector(dtype_exclude="category"),
            ),
            remainder="passthrough",
        )
        regressor = XGBRegressor()
        pipe = make_pipeline(one_hot_encoder, regressor)
        return pipe

    def train_model(self):
        X_train, y_train, _, _ = self._split_data()
        pipe = self._get_model_pipeline()
        pipe.fit(X_train, y_train)
        self.model = pipe

    def evaluate_model(self):
        _, _, X_test, y_test = self._split_data()
        pipe = self.model
        return pipe.score(X_test, y_test)
