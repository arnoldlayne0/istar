import pandera as pa
from pandera.typing import Series

COLUMNS_FROM_PLAYERS_TO_FPL_DATASET = [
    "chance_of_playing_next_round",
    "chance_of_playing_this_round",
    "element_type",
    "first_name",
    "second_name",
    "player_id",
    "now_cost",
    "selected_by_percent",
    "team_code",
    "player_name",
]
COLUMNS_FROM_PLAYER_HISTORIES_TO_FPL_DATASET = [
    "player_id",
    "opponent_team",
    "total_points",
    "was_home",
    "kickoff_time",
    "team_h_score",
    "team_a_score",
    "gameweek",
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
    "expected_goals",
    "expected_assists",
    "expected_goal_involvements",
    "expected_goals_conceded",
    "value",
    "transfers_balance",
    "selected",
    "transfers_in",
    "transfers_out",
]
COLUMNS_FROM_TEAMS_TO_FPL_DATASET = [
    "team_code",
    "team_id",
    "team_name",
    "team_name_short",
]
COLUMNS_TO_FPL_DATASET = {
    "players": COLUMNS_FROM_PLAYERS_TO_FPL_DATASET,
    "player_histories": COLUMNS_FROM_PLAYER_HISTORIES_TO_FPL_DATASET,
    "teams": COLUMNS_FROM_TEAMS_TO_FPL_DATASET,
}


class PlayersSchema(pa.SchemaModel):
    pass


class PlayerHistoriesSchema(pa.SchemaModel):
    pass


class PlayerFixturesSchema(pa.SchemaModel):
    pass


class TeamsSchema(pa.SchemaModel):
    pass


class FDRSchema(pa.SchemaModel):
    pass


class FPLDatasetSchema(pa.SchemaModel):
    """
    Some columns are somewhat problematic:
    - now_cost and selected_by_percent give the latest values even for past gameweeks
      because the dataset set is overwritten each time
      (and gets those values from the players dataset)
    - chance of playing values are often null, not sure if the problem is when they get updated?
    """

    player_id: Series[pa.typing.Int64]
    opponent_team: Series[pa.typing.Int64]
    total_points: Series[pa.typing.Int64]
    was_home: Series[pa.typing.Bool]
    team_h_score: Series[pa.typing.Int64] = pa.Field(nullable=True)
    team_a_score: Series[pa.typing.Int64] = pa.Field(nullable=True)
    gameweek: Series[pa.typing.Int64] = pa.Field(ge=1, le=38)
    minutes: Series[pa.typing.Int64]
    goals_scored: Series[pa.typing.Int64]
    assists: Series[pa.typing.Int64]
    clean_sheets: Series[pa.typing.Int64]
    goals_conceded: Series[pa.typing.Int64]
    own_goals: Series[pa.typing.Int64]
    penalties_saved: Series[pa.typing.Int64]
    penalties_missed: Series[pa.typing.Int64]
    yellow_cards: Series[pa.typing.Int64]
    red_cards: Series[pa.typing.Int64]
    saves: Series[pa.typing.Int64]
    bonus: Series[pa.typing.Int64]
    bps: Series[pa.typing.Int64]
    influence: Series[pa.typing.Float64]
    creativity: Series[pa.typing.Float64]
    threat: Series[pa.typing.Float64]
    ict_index: Series[pa.typing.Float64]
    expected_goals: Series[pa.typing.Float64]
    expected_assists: Series[pa.typing.Float64]
    expected_goal_involvements: Series[pa.typing.Float64]
    expected_goals_conceded: Series[pa.typing.Float64]
    value: Series[pa.typing.Int64]
    transfers_balance: Series[pa.typing.Int64]
    selected: Series[pa.typing.Int64]
    transfers_in: Series[pa.typing.Int64]
    transfers_out: Series[pa.typing.Int64]
    chance_of_playing_next_round: Series[pa.typing.Float64] = pa.Field(nullable=True)
    chance_of_playing_this_round: Series[pa.typing.Float64] = pa.Field(nullable=True)
    element_type: Series[pa.typing.Int64]
    first_name: Series[pa.typing.String]
    now_cost: Series[pa.typing.Int64]
    selected_by_percent: Series[pa.typing.Float64]
    team_code: Series[pa.typing.Int64]
    player_name: Series[pa.typing.String]
    team_id: Series[pa.typing.Int64]
    team_name: Series[pa.typing.String]
    team_name_short: Series[pa.typing.String]
