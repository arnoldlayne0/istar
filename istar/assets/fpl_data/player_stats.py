from dataclasses import dataclass
from enum import Enum
from typing import Tuple

import numpy as np
import pandas as pd


STATS_WINDOW = 5


class PlayerCharacteristicsEnum(Enum):
    element_type = 1
    team_id = 2


class FixtureCharacteristicsEnum(Enum):
    opponent_team = 0
    was_home = 1


class PlayerStatsEnum(Enum):
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


class IdentifierEnum(Enum):
    player_id = 0
    gameweek = 1


class LabelEnum(Enum):
    total_points_current = 0
    total_points_next_5 = 1
    total_points_weighted = 2


class ModifierEnum(Enum):
    chance_of_playing_this_round = 0
    chance_of_playing_next_round = 1


class PlayerStatistics:
    def __init__(self, fpl_dataset: pd.DataFrame):
        self.fpl_dataset = fpl_dataset
        self.player_stats_names = [e.name for e in PlayerStatsEnum]
        self.stats_window = STATS_WINDOW

    def _get_characteristics(self) -> pd.DataFrame:
        return self.fpl_dataset[
            [e.name for e in FixtureCharacteristicsEnum]
            + [e.name for e in PlayerCharacteristicsEnum]
            + [e.name for e in IdentifierEnum]
        ]

    def get_player_stats(self) -> pd.DataFrame:
        rolling_stats = self._compute_rolling_stats(
            window=self.stats_window,
            stats_names=self.player_stats_names,
            from_future=False,
        )
        # cumul_stats =
        return rolling_stats

    def _compute_rolling_stats(self, window, stats_names, from_future) -> pd.DataFrame:
        """
        Compute rolling stats for each player in the dataset.
        Stats are inclusive, that is, for gameweek N,
        rolling stats are computed from player data for gameweeks N, N-1, ..., N-window+1.
        """
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
        return points_by_team_element_expanding.rename(
            columns={"expanding_mean_points": f"total_points_{prefix}_team_element"}
        )
