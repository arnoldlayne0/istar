from dagster import asset, AssetIn, OpExecutionContext, Output
import pandas as pd
from fpl import FPL
import aiohttp

from sklearn.pipeline import Pipeline

from istar.assets.fpl_data.data_helpers import (
    get_player_name_hash,
    assign_effective_team_id,
)
from istar.assets.fpl_data.player_stats import PlayerStatistics
from istar.assets.fpl_data.training_data import TrainingData
from istar.data.schemas import FPLDatasetSchema, COLUMNS_TO_FPL_DATASET
from istar.assets.fpl_data.models import SklearnModel


@asset(io_manager_key="pandas_local_io_manager")
async def players() -> Output[pd.DataFrame]:
    # consider appending with each new gameweek (would need player_fixtures to see gameweek?)
    # would be annoying for many reasons, eg schema evolution or failures in a given gameweek
    async with aiohttp.ClientSession() as session:
        fpl_session = FPL(session)
        players_json = await fpl_session.get_players(return_json=True)
        players_pd = pd.DataFrame(players_json).rename(
            columns={"id": "player_id", "web_name": "player_name"}
        )
        return Output(players_pd, metadata={"num_rows": players_pd.shape[0]})


@asset(ins={"players": AssetIn("players")}, io_manager_key="pandas_local_io_manager")
async def player_histories(players: pd.DataFrame) -> Output[pd.DataFrame]:
    player_ids = players["player_id"].unique().tolist()
    async with aiohttp.ClientSession() as session:
        fpl_session = FPL(session)
        player_summaries = await fpl_session.get_player_summaries(
            player_ids=player_ids, return_json=True
        )
        histories = []
        for player_summary in player_summaries:
            histories.append(
                pd.DataFrame(player_summary["history"]).rename(
                    columns={"element": "player_id"}
                )
            )
        all_histories_pd = pd.concat(histories)
        all_histories_pd = all_histories_pd.rename(columns={"round": "gameweek"})

        all_histories_pd = all_histories_pd[~all_histories_pd["team_h_score"].isna()]
        all_histories_pd = all_histories_pd.drop_duplicates(
            subset=["player_id", "gameweek"], keep="first"
        )

        return Output(
            all_histories_pd,
            metadata={
                "num_rows": all_histories_pd.shape[0],
                "min_round": int(all_histories_pd["gameweek"].min()),
                "max_round": int(all_histories_pd["gameweek"].max()),
            },
        )


@asset(io_manager_key="pandas_local_io_manager")
async def fixtures() -> Output[pd.DataFrame]:
    async with aiohttp.ClientSession() as session:
        fpl_session = FPL(session)
        all_fixtures = await fpl_session.get_fixtures(return_json=True)
        fixtures_pd = pd.DataFrame(all_fixtures)
        next_gameweek = fixtures_pd[fixtures_pd["started"] == False]["event"].min()
        return Output(
            fixtures_pd,
            metadata={
                "num_rows": fixtures_pd.shape[0],
                "next_gameweek": int(next_gameweek)},
        )


@asset(io_manager_key="pandas_local_io_manager")
async def teams() -> Output[pd.DataFrame]:
    async with aiohttp.ClientSession() as session:
        fpl_session = FPL(session)
        teams_json = await fpl_session.get_teams(return_json=True)
        teams_pd = pd.DataFrame(teams_json).rename(
            columns={
                "id": "team_id",
                "code": "team_code",
                "name": "team_name",
                "short_name": "team_name_short",
            }
        )
        return Output(teams_pd, metadata={"num_rows": teams_pd.shape[0]})


@asset(
    ins={
        "players": AssetIn("players"),
        "player_histories": AssetIn("player_histories"),
        "teams": AssetIn("teams"),
    },
    io_manager_key="pandas_local_io_manager",
)
def fpl_dataset(
    players: pd.DataFrame, player_histories: pd.DataFrame, teams: pd.DataFrame
) -> Output[pd.DataFrame]:
    players_to_join = players[COLUMNS_TO_FPL_DATASET["players"]]
    player_histories_to_join = player_histories[
        COLUMNS_TO_FPL_DATASET["player_histories"]
    ]
    teams_to_join = teams[COLUMNS_TO_FPL_DATASET["teams"]]
    fpl_dataset_pd = players_to_join.merge(
        player_histories_to_join, on="player_id", how="inner"
    ).merge(teams_to_join, on="team_code", how="inner")

    # explicitly cast some types
    fpl_dataset_pd["team_h_score"] = fpl_dataset_pd["team_h_score"].astype("Int64")
    fpl_dataset_pd["team_a_score"] = fpl_dataset_pd["team_a_score"].astype("Int64")

    # create some new ids
    fpl_dataset_pd["player_names_hash"] = fpl_dataset_pd.apply(
        get_player_name_hash, axis=1
    )
    fpl_dataset_pd["effective_team_id"] = assign_effective_team_id(fpl_dataset_pd)

    # TODO: add next fixture (fixtures later)
    # fixtures_cross = players_to_join.merge(fixtures, how="cross")
    # fixtures_inner = fixtures_cross[fixtures_cross.apply(lambda r: r["team"] == r["team_a"] or r["team"] == r["team_h"], axis=1)]
    # fpl_dataset_pf.append(fixtures_inner, ignore_index=True

    # Validation behaving weird - commented out for now
    # FPLDatasetSchema.validate(fpl_dataset_pd)
    return Output(
        fpl_dataset_pd,
        metadata={
            # list missing gameweeks
            "num_rows": fpl_dataset_pd.shape[0],
            "num_cols": fpl_dataset_pd.shape[1],
            "dtypes": {k: str(v) for k, v in fpl_dataset_pd.dtypes.to_dict().items()},
        },
    )


@asset(
    ins={"fpl_dataset": AssetIn("fpl_dataset")},
    io_manager_key="pandas_local_io_manager",
)
def player_stats(fpl_dataset: pd.DataFrame) -> Output[pd.DataFrame]:
    """Create player stats from the FPL datasets."""
    ps = PlayerStatistics(fpl_dataset=fpl_dataset)
    player_stats_pd = ps.get_player_stats()
    return Output(
        player_stats_pd,
        metadata={
            "num_rows": player_stats_pd.shape[0],
            "num_columns": player_stats_pd.shape[1],
        },
    )


@asset(
    ins={
        "fpl_dataset": AssetIn("fpl_dataset"),
        "player_stats": AssetIn("player_stats"),
    },
    io_manager_key="pandas_local_io_manager",
)
def training_data(fpl_dataset: pd.DataFrame, player_stats: pd.DataFrame) -> Output[pd.DataFrame]:
    """Create training data from the FPL datasets."""
    # read stats here and join to other needed stuff
    # and shift stats to match the labels
    td = TrainingData(fpl_dataset=fpl_dataset)
    training_data_pd = td.get_train_test_split()

    return Output(
        training_data_pd,
        metadata={
            "num_rows": training_data_pd.shape[0],
            "num_columns": training_data_pd.shape[1],
        },
    )


@asset(
    ins={"training_data": AssetIn("training_data")},
    io_manager_key="joblib_local_io_manager",
)
def model_pipeline(training_data: pd.DataFrame):
    """Create training data from the FPL datasets."""
    model = SklearnModel(data=training_data)
    model.train_model()
    test_score = model.evaluate_model()

    return Output(model.model, metadata={"test_score": test_score})


@asset(
    ins={"fixtures": AssetIn("fixtures")},
    io_manager_key="pandas_local_io_manager",
)
def predictions(fixtures: pd.DataFrame) -> Output[pd.DataFrame]:
    # preprocess_fixture_data = PreprocessFixtureData(fixtures)
    # load model
    # predict
    pass
