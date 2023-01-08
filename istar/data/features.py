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


def add_cumulative_features(players_pd: pd.DataFrame) -> pd.DataFrame:
    players_pd = players_pd.sort_values(["player_id", "fixture"], ascending=True)

    for feature in CUMUL_FEATURES:
        players_pd[f"{feature}_cumul"] = players_pd.groupby(["player_id"])[
            feature
        ].cumsum(skipna=False)

    return players_pd
