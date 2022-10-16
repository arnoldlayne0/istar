from typing import List, Union
import logging
import asyncio
import aiohttp
import pandas as pd

from fpl import FPL
from sqlalchemy import create_engine


FPL_SEASON = "season_22_23"
BASE_SAVE_PATH = f"/Users/milosztaracha/projects/istar/data/{FPL_SEASON}/raw"
logging.basicConfig(level=logging.INFO)


def extract_players_current_summaries(players: List) -> pd.DataFrame:
    players_pd = pd.DataFrame(players).rename(columns={"id": "player_id"})
    return players_pd


def extract_player_ids(players_pd: pd.DataFrame) -> List:
    return players_pd["player_id"].to_list()


def extract_players_histories_and_fixtures(
    player_summaries: List,
) -> List[Union[pd.DataFrame, pd.Series]]:
    histories, fixtures, histories_past = [], [], []

    for player_summary in player_summaries:
        cur_player_id = player_summary["history"][0]["element"]

        history_pd = pd.DataFrame(player_summary["history"]).rename(columns={"element": "player_id"})
        fixtures_pd = pd.DataFrame(player_summary["fixtures"])
        history_past_pd = pd.DataFrame(player_summary["history_past"])

        fixtures_pd["player_id"] = cur_player_id
        history_past_pd["player_id"] = cur_player_id

        histories.append(pd.DataFrame(history_pd))
        fixtures.append(pd.DataFrame(fixtures_pd))
        histories_past.append(pd.DataFrame(history_past_pd))

    return [pd.concat(histories), pd.concat(fixtures), pd.concat(histories_past)]


def get_current_gameweek(players_history_pd: pd.DataFrame) -> int:
    return players_history_pd["round"].max()


def write_to_csv_and_db(df_to_write: pd.DataFrame, table_name: str, engine) -> None:
    csv_path = f"{BASE_SAVE_PATH}/{table_name}.csv"
    df_to_write = df_to_write.assign(fpl_season=FPL_SEASON)
    try:
        df_to_write.to_csv(csv_path)
        logging.info("written %s to csv", table_name)
    except Exception:
        logging.exception("failed to write %s to csv", table_name)

    try:
        df_to_write.to_sql(table_name, con=engine, if_exists="replace")
        logging.info("written %s to db", table_name)
    except Exception:
        logging.exception("failed to write %s to db", table_name)


async def main():
    engine = create_engine("duckdb:////Users/milosztaracha/projects/istar/db/rhun.db")
    async with aiohttp.ClientSession() as session:
        fpl = FPL(session)
        logging.info("Started FPL session")

        players = await fpl.get_players(return_json=True)
        players_pd = extract_players_current_summaries(players)
        player_ids = extract_player_ids(players_pd)
        player_summaries = await fpl.get_player_summaries(
            player_ids=player_ids, return_json=True
        )
        player_details = extract_players_histories_and_fixtures(player_summaries)
        players_history_pd, players_fixtures_pd, players_history_past = player_details
        teams = await fpl.get_teams(return_json=True)
        teams_pd = pd.DataFrame(teams).rename(columns={"id": "team_id"})

        current_gameweek = get_current_gameweek(players_history_pd)
        logging.info("Writing data for gameweek %s", current_gameweek)

        dfs = [
            players_pd,
            players_history_pd,
            players_fixtures_pd,
            players_history_past,
            teams_pd,
        ]
        table_names = [
            "players_current_summary",
            "players_history",
            "players_fixtures",
            "players_history_past",
            "teams",
        ]
        for df, table_name in zip(dfs, table_names):
            write_to_csv_and_db(df, table_name, engine)
        logging.info("Downloaded and saved all datasets")


if __name__ == "__main__":
    asyncio.run(main())
