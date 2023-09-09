import hashlib

import numpy as np
import numpy.typing as npt
import pandas as pd

PROMOTED_TEAMS_22_23 = pd.DataFrame(
    {
        "team_name": ["Bournemouth", "Fulham", "Nott'm Forest"],
        "team_code": [91, 54, 17],
    }
)

PROMOTED_TEAMS_23_24 = pd.DataFrame(
    {
        "team_name": ["Burnley", "Luton", "Sheffield United"],
        "team_code": [90, 102, 49],
    }
)


def assign_effective_team_id(df: pd.DataFrame) -> npt.NDArray[np.int64]:
    conditions = [
        df["team_code"] == 91,
        df["team_code"] == 54,
        df["team_code"] == 17,
    ]
    choices = [90, 102, 49]
    return np.select(conditions, choices, default=df["team_code"])


def get_player_name_hash(row: pd.Series) -> int:
    s = row["first_name"] + row["second_name"]
    return int(hashlib.sha256(s.encode("utf-8")).hexdigest(), 16) % 10**8
