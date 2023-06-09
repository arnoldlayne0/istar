from dataclasses import dataclass
from enum import Enum
from typing import Tuple

import numpy as np
import pandas as pd


STATS_WINDOW = 5


class PlayerCharacteristics(Enum):
    element_type = 1
    team_id = 2


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
        self.stats_window = STATS_WINDOW

    def _get_characteristics(self) -> pd.DataFrame:
        return self.fpl_dataset[
            [e.name for e in FixtureCharacteristics]
            + [e.name for e in PlayerCharacteristics]
            + [e.name for e in Identifier]
        ]

    def _get_player_stats(self) -> pd.DataFrame:
        rolling_stats = self._compute_rolling_stats(
            window=self.stats_window,
            stats_names=self.player_stats_names,
            from_future=False,
        ).assign(gameweek=lambda df: df.gameweek + 1)
        return rolling_stats

    def _get_labels(self) -> pd.DataFrame:
        future_points = self._compute_rolling_stats(
            window=self.stats_window, stats_names=["total_points"], from_future=True
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

    def get_dataset(self) -> pd.DataFrame:
        characteristics = self._get_characteristics()
        player_stats = self._get_player_stats()
        points_against_team = self._compute_points_by_team_and_element(
            points_against=True
        )
        points_for_team = self._compute_points_by_team_and_element(points_against=False)
        labels = self._get_labels()
        dataset = (
            characteristics.merge(
                player_stats, on=["player_id", "gameweek"], how="inner"
            )
            .merge(labels, on=["player_id", "gameweek"], how="inner")
            .merge(
                points_against_team,
                on=["gameweek", "opponent_team", "element_type"],
                how="inner",
            )
            .merge(
                points_for_team, on=["gameweek", "team_id", "element_type"], how="inner"
            )
        )
        return dataset

    def get_train_test_split(self, train_size=0.8) -> pd.DataFrame:
        dataset = self.get_dataset()
        conditions = [
            (dataset["gameweek"] <= self.stats_window),
            (dataset["gameweek"] > self.stats_window)
            & (dataset["gameweek"] <= train_size * dataset["gameweek"].max()),
            (dataset["gameweek"] > train_size * dataset["gameweek"].max()),
        ]
        choices = ["cold_start", "train", "test"]
        dataset["set"] = np.select(conditions, choices)
        return dataset

    def _get_label_weights(self) -> pd.DataFrame:
        max_gw = self.fpl_dataset.gameweek.max()
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
        )[: max_gw + 1]
        return pd.DataFrame({"gameweek": np.arange(1, max_gw + 1), "weight": weights})

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

    def _compute_points_by_team_and_element(self, points_against=True):
        if points_against:
            team = "opponent_team"
            prefix = "against"
        else:
            team = "team_id"
            prefix = "by"
        dataset = self.fpl_dataset
        points_by_team_element = (
            dataset[dataset["minutes"] > 15]
            .groupby(["gameweek", f"{team}", "element_type"], as_index=False)[
                "total_points"
            ]
            .mean()
            .sort_values("gameweek")
        )
        points_by_team_element_expanding = (
            points_by_team_element.sort_values([f"{team}", "element_type", "gameweek"])
            .groupby([f"{team}", "element_type"])
            .expanding()
            .agg({"total_points": "mean", "gameweek": "max"})
            .reset_index()
            .drop(columns=["level_2"])
            .rename(columns={"total_points": "expanding_mean_points"})
        )
        points_by_team_element_expanding = points_by_team_element_expanding.merge(
            points_by_team_element, on=["gameweek", f"{team}", "element_type"]
        ).drop(columns=["total_points"])
        points_by_team_element_expanding["gameweek"] = (
            points_by_team_element_expanding["gameweek"] + 1
        )
        return points_by_team_element_expanding.rename(
            columns={"expanding_mean_points": f"total_points_{prefix}_team_element"}
        )
