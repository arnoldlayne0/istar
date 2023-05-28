from typing import List
import pandas as pd


CUMUL_FEATURES = [
    "total_points",
    "minutes",
    "goals_scored",
    "assists",
    "clean_sheets",
    "goals_conceded",
    "own_goals",
    "penalties_saved",
    "penalties_missed",
    "yellow_cards",
    "red_cards",
    "saves",
    "bonus",
    "bps",
    "influence",
    "creativity",
    "threat",
    "ict_index",
    "starts",
    "expected_goals",
    "expected_assists",
    "expected_goal_involvements",
    "expected_goals_conceded",
    "transfers_balance",
    "selected",
    "transfers_in",
    "transfers_out",
]


class Feature:
    def __init__(self, name: str, func, **kwargs):
        self.name = name
        self.kwargs = kwargs

    def _compute(self) -> pd.DataFrame:
        raise NotImplementedError
    
    def add_features_to_dataset(self) -> pd.DataFrame:
        raise NotImplementedError
    

class RollingFeatures(Feature):
    def __init__(self, dataset: pd.DataFrame, window: int, stats: List):
        self.dataset = dataset
        self.window = window
        self.stats = stats

    def _compute(self) -> pd.DataFrame: 
        rolling_stats = (
            self.dataset.sort_values("gameweek", ascending=True)
                .groupby(["player_id"])
                .rolling(window=self.window, min_periods=self.window, on="gameweek")[self.stats]
                .mean()
                .reset_index()
                .rename(columns={stat: f"{stat}_rolling_{self.window}" for stat in self.stats})
        )
        return rolling_stats
    
    def add_features_to_dataset(self) -> pd.DataFrame:
        features = self._compute()
        return self.dataset.merge(features, on=["player_id", "gameweek"])
    

class CumulativeFeatures(Feature):
    def __init__(self, dataset: pd.DataFrame, stats: List):
        self.dataset = dataset
        self.stats = stats

    def _compute(self) -> pd.DataFrame: 
        cumulative_stats = (
            self.dataset.groupby('player_id', sort=False)[self.stats]
            .transform(lambda x: x.expanding().mean())
        )
        return cumulative_stats
    
    def add_features_to_dataset(self) -> pd.DataFrame:
        features = self._compute()
        return self.dataset.join(features, rsuffix="_cumulative")
