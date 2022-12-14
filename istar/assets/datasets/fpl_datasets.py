from dagster import asset, AssetIn, Output
import pandas as pd
from fpl import FPL
import aiohttp
from istar.data.schemas import FPLDatasetSchema, COLUMNS_TO_FPL_DATASET


@asset
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


@asset(ins={"players": AssetIn("players")})
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
        return Output(
            all_histories_pd,
            metadata={
                "num_rows": all_histories_pd.shape[0],
                "min_round": int(all_histories_pd["gameweek"].min()),
                "max_round": int(all_histories_pd["gameweek"].max()),
            },
        )


@asset(ins={"players": AssetIn("players")})
async def player_fixtures(players: pd.DataFrame) -> Output[pd.DataFrame]:
    player_ids = players["player_id"].unique().tolist()
    async with aiohttp.ClientSession() as session:
        fpl_session = FPL(session)
        player_summaries = await fpl_session.get_player_summaries(
            player_ids=player_ids, return_json=True
        )
        fixtures = []
        for player_summary in player_summaries:
            fixtures.append(pd.DataFrame(player_summary["fixtures"]))
        all_fixtures_pd = pd.concat(fixtures)
        return Output(
            all_fixtures_pd,
            metadata={
                "num_rows": all_fixtures_pd.shape[0],
                "min_round": int(all_fixtures_pd["event"].min()),
                "last_round": int(all_fixtures_pd["event"].max()),
            },
        )


@asset
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


@asset
async def fdr() -> Output[pd.DataFrame]:
    async with aiohttp.ClientSession() as session:
        fpl_session = FPL(session)
        fdr_json = await fpl_session.FDR()
        fdr_pd = pd.DataFrame(fdr_json)
        return Output(fdr_pd, metadata={"num_rows": fdr_pd.shape[0]})


@asset(
    ins={
        "players": AssetIn("players"),
        "player_histories": AssetIn("player_histories"),
        "teams": AssetIn("teams"),
    }
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
    FPLDatasetSchema.validate(fpl_dataset_pd)
    return Output(
        fpl_dataset_pd,
        metadata={
            "num_rows": fpl_dataset_pd.shape[0],
            "num_cols": fpl_dataset_pd.shape[1],
        },
    )
