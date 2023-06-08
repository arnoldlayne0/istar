from enum import Enum

import numpy as np
import pandas as pd


class PlayerCharacteristics(Enum):
    element_type = 1
    team_code = 2


class FixtureCharacteristics(Enum):
    opponent_team = 0
    was_home = 1


class PlayerStats(Enum):
    selected_by_percent = 0
    total_points = 1
    bps = 2
    influence = 3
    creativity = 4
    threat = 5
    expected_goal_involvements = 6
    expected_goals_conceded = 7
    value = 8
    transfers_balance = 9


class Identifier(Enum):
    player_id = 0
    gameweek = 1


class Label(Enum):
    total_points_current = 0
    total_points_next_5 = 1
    total_points_weighted = 2


class Modifier(Enum):
    chance_of_playing_this_round = 0
    chance_of_playing_next_round = 1


class TrainingData:
    def __init__(self, fpl_dataset: pd.DataFrame):
        self.fpl_dataset = fpl_dataset
        self.player_stats_names = [e.name for e in PlayerStats]

    def _get_characteristics(self) -> pd.DataFrame:
        return self.fpl_dataset[
            [e.name for e in FixtureCharacteristics]
            + [e.name for e in PlayerCharacteristics]
            + [e.name for e in Identifier]
        ]

    def _get_player_stats(self) -> pd.DataFrame:
        rolling_stats = self._compute_rolling_stats(
            window=5, stats_names=self.player_stats_names, from_future=False
        ).assign(gameweek=lambda df: df.gameweek + 1)
        return rolling_stats

    def _get_labels(self) -> pd.DataFrame:
        future_points = self._compute_rolling_stats(
            window=5, stats_names=["total_points"], from_future=True
        )
        current_points = self.fpl_dataset[["player_id", "total_points", "gameweek"]]
        label_weights = self._get_label_weights()
        labels = future_points.merge(
            current_points, on=["player_id", "gameweek"], how="inner"
        ).merge(label_weights, on="gameweek", how="inner")
        labels["weighted_label"] = labels["total_points"] * labels["weight"] + labels[
            "total_points_rolling_next_5"
        ] * (1 - labels["weight"])
        return labels

    def get_training_dataset(self) -> pd.DataFrame:
        characteristics = self._get_characteristics()
        player_stats = self._get_player_stats()
        labels = self._get_labels()
        dataset = characteristics.merge(
            player_stats, on=["player_id", "gameweek"], how="inner"
        ).merge(labels, on=["player_id", "gameweek"], how="inner")
        return dataset

    @staticmethod
    def _get_label_weights() -> pd.DataFrame:
        opening_gws = np.linspace(0.5, 1, 4)
        before_reset_gws = np.linspace(0.25, 1, 5)
        after_reset_gws = np.array([0.25] * 12)
        weights = np.concatenate(
            [
                opening_gws,
                before_reset_gws,
                after_reset_gws,
                before_reset_gws,
                after_reset_gws,
            ]
        )
        return pd.DataFrame({"gameweek": np.arange(1, 39), "weight": weights})

    def _compute_rolling_stats(self, window, stats_names, from_future) -> pd.DataFrame:
        sort_ascending = not from_future
        if from_future:
            prefix = "next"
        else:
            prefix = "previous"
        rolling_stats = (
            self.fpl_dataset.sort_values("gameweek", ascending=sort_ascending)
            .groupby(["player_id"])
            .rolling(window=window, min_periods=window, on="gameweek")[stats_names]
            .mean()
            .reset_index()
            .rename(
                columns={
                    stat: f"{stat}_rolling_{prefix}_{window}" for stat in stats_names
                }
            )
        )
        return rolling_stats
