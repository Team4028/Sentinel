from enum import Enum
from functools import reduce
import operator
import sqlite3
import time
from typing import Any, Callable, TypeVar

import pandas as pd
import os
import json
import numpy as np

try:
    from lib.data_config import eval_beakscript, FANCY_FIL
    import apputils
except ModuleNotFoundError:
    from src.lib.data_config import eval_beakscript, FANCY_FIL
    import src.apputils
from collections import defaultdict
from collections.abc import Iterable
import logging
from scipy.linalg import svd
import statbotics

logger = logging.getLogger(__name__)


class TeamStruct:
    def __init__(self) -> None:
        self.data = {}

    def extend_data(self, data) -> None:
        """Append a match of data to the team. Everything is procedural because of field-config"""
        merged = defaultdict(list)
        for d in [self.data, data]:
            for k, v in d.items():
                try:
                    merged[k].extend(v)
                except:
                    merged[k].append(v)
        self.data = dict(merged)

    def output_dict(self, config):
        """Get all of the team data as a dictionary, unwrapping DataFields to get averages and such"""
        data = {}
        # the | operator for dicts in python combines dicts with different entries (OR's them)
        for field in config["svd"]:
            data |= (
                DataField(field["name"], self.data[field["name"]], []).objectify()
                | DataField(
                    f"{field["name"]} Variance",
                    self.data[f"{field["name"]} Variance"],
                    [],
                ).objectify()
            )
        for field in config["teams"]:
            data |= DataField(
                field["name"], self.data[field["name"]], field["filters"]
            ).objectify()
        return data


class DataField:
    def __init__(self, name, data, filters) -> None:
        self.name = name
        self.filters = filters
        self.data = data

    def average(self) -> float | str:
        """returns the average of the dataset"""
        return (
            # x̄ = Σx/|x|
            round(sum(self.data) / len(self.data), 3)
            if len(self.data) > 0
            else "N/A"
        )

    def max(self):
        """returns the max of the dataset"""
        return max(self.data) if len(self.data) > 0 else "N/A"

    def filter(self) -> float:
        """returns the average of the dataset, filtered by median += 2 * MAD"""
        return float(
            np.around(np.mean(Processor.mad_filter(np.array(self.data))), 3)
        )  # around is just round

    calc_map = {"avg": average, "max": max, "fil": filter}

    def objectify(self):
        """calculates the applicable filters to serialize the data in this field into a dict"""
        data = {}
        if len(self.filters) > 0:
            for fil in self.filters:
                data[FANCY_FIL[fil] + " " + self.name] = self.calc_map[fil](
                    self
                )  # call the cooresponding function to apply the filter
        elif (
            isinstance(self.data, list) and len(self.data) == 1
        ):  # if no filters, passthrough
            data[self.name] = self.data[0]
        else:
            data[self.name] = self.data  # if the data is just a float, use it
        return data


class MatchStruct:
    def __init__(self) -> None:
        self.teams = {}

    def add_team_data(self, team, data) -> None:
        """add data from a team for this match"""
        self.teams.setdefault(team, data)  # use setdefault to create the team if it DNE

    def output_dict(self, config):
        """serialize the struct into a dictionary"""
        data = []
        for team in self.teams.keys():
            d2 = {"Team": team}  # add an entry for what team it is
            for field in config["matches"]:
                d2 |= DataField(
                    field["name"], self.teams[team][field["name"]], field["filters"]
                ).objectify()
            data.append(d2)
        return data


ET = TypeVar("ET", bound=Callable)


class Event[ET]:

    def __is_iterable(x):
        try:
            iter(x)
            return True
        except TypeError:
            return False

    def __init__(self, name: str = ""):
        self._handlers = []
        self.name = name

    def __iadd__(self, f: ET | list[ET]):
        if Event.__is_iterable(f):
            self._handlers.extend(f)
        else:
            self._handlers.append(f)
        return self

    def __isub__(self, f: ET | list[ET]):
        if Event.__is_iterable(f):
            self._handlers.extend(f)
        else:
            self._handlers.remove(f)
        return self

    def fire(self, logging_callback: Callable[[str], None], *args, **kwargs):
        for i, f in enumerate(self._handlers):
            if logging_callback != None:
                logging_callback(
                    f"{self.name} [{i + 1}/{len(self._handlers)}]: {f.__name__}"
                )
            f(*args, **kwargs)


class ObjectHolder[T]:
    """Pass-by-reference for noobs"""

    def __init__(self, object: T):
        self.obj = object


class Processor:
    """Main class of data calculation, handles all calculation basically"""

    last_fetch_timestamp = 0

    def __init__(
        self,
        outpath,
        outname,
        period_min,
        event_key,
        tba_key,
        year,
        chunk_size,
        config_data,
    ) -> None:
        self.chunk_size = chunk_size
        self.outpath = outpath
        self.outname = outname
        self.database_name = "sentinel.db" # TODO: make setting for this
        self.period_min = period_min
        self._tba_data = apputils.TBAData()
        self.event_key, self.tba_key, self.year = event_key, tba_key, year
        self._teams: dict[str, TeamStruct] = {}
        self.sb = statbotics.Statbotics()
        self._sb_epas = {}
        self._sb_matches = []
        self._matches = {}
        self._epas = {}
        self.config_data = config_data
        self.periodic_calls = Event[Callable[[], None]]("Periodic Fetch Routine")
        self.chunk_processing_routine = Event[
            Callable[[ObjectHolder[pd.DataFrame]], None]
        ]("Processing Routine")
        self.post_process_routine = Event[Callable[[], None]]("Post-processing Routine")

        self.periodic_calls += [
            self.load_remote_data,
            self.write_match_schedule_file,
            self.write_teams_file,
            self.write_statbotics_analytics,
            self.write_team_tba_data_file,
            self.reset_fetch_timestamp,
        ]

        self.chunk_processing_routine += [
            self.pre_filter_chunk,
            self.pre_process_chunk,
            self.drop_duplicates_chunk,
            self.filter_chunk,
            self.compute_fields_chunk,
            self.build_teams_chunk,
            self.svd_data_chunk,
            self.team_proc_chunk,
            self.match_proc_chunk,
        ]

        self.post_process_routine += [
            self.write_match_predictions_file,
            self.write_match_depth_predictions,
            self.write_statbotics_epa,
            self.write_match_fields,
            self.write_team_fields,
        ]

        self.sql_fields = {
            "matches": [
                "Red_1",
                "Red_2",
                "Red_3",
                "Blue_1",
                "Blue_2",
                "Blue_3",
                "Predict_R1_Score",
                "Predict_R2_Score",
                "Predict_R3_Score",
                "Predict_B1_Score",
                "Predict_B2_Score",
                "Predict_B3_Score",
                "Predict_Red_Score",
                "Predict_Blue_Score",
                "Predict_Winner",
                "Statbotics_winner",
                "Statbotics_red_win_prob",
                "Statbotics_red_Score",
                "Statbotics_blue_Score",
                "Statbotics_red_energized_rp",
                "Statbotics_blue_energized_rp",
                "Statbotics_red_supercharged_rp",
                "Statbotics_blue_supercharged_rp",
                "Statbotics_red_traversal_rp",
                "Statbotics_blue_traversal_rp",
                "Statbotics_red_rp_1",
                "Statbotics_blue_rp_1",
                "Statbotics_red_rp_2",
                "Statbotics_blue_rp_2",
                "Statbotics_red_rp_3",
                "Statbotics_blue_rp_3",
            ],
            "matches_depth_predictions": [
                "attr_name",
                "attr_value",
            ],
            "matches_fields": [
                "field_name",
                "field_value",
            ],
            "teams": [
                "EPA",
                "Rank",
                "Average_RP",
                "OPR",
                "Last_OPR",
                "Country",
                "State",
                "City",
                "Name",
                "School",
                "RookieYear",
                "PostalCode",
                "Website"
            ],
            "teams_fields": [
                "field_name",
                "field_value",
            ],
        }

        # Create Tables
        with sqlite3.connect(
            os.path.join(self.outpath, self.database_name)
        ) as conn:
            conn.executescript(
                f"""
                CREATE TABLE IF NOT EXISTS matches (
                    Match TEXT PRIMARY KEY,
                    Red_1 INT,
                    Red_2 INT,
                    Red_3 INT,
                    Blue_1 INT,
                    Blue_2 INT,
                    Blue_3 INT,
                    Predict_R1_Score REAL,
                    Predict_R2_Score REAL,
                    Predict_R3_Score REAL,
                    Predict_B1_Score REAL,
                    Predict_B2_Score REAL,
                    Predict_B3_Score REAL,
                    Predict_Red_Score REAL,
                    Predict_Blue_Score REAL,
                    Predict_Winner TEXT,
                    Statbotics_winner TEXT,
                    Statbotics_red_win_prob REAL,
                    Statbotics_red_Score REAL,
                    Statbotics_blue_Score REAL,
                    Statbotics_red_energized_rp REAL,
                    Statbotics_blue_energized_rp REAL,
                    Statbotics_red_supercharged_rp REAL,
                    Statbotics_blue_supercharged_rp REAL,
                    Statbotics_red_traversal_rp REAL,
                    Statbotics_blue_traversal_rp REAL,
                    Statbotics_red_rp_1 REAL,
                    Statbotics_blue_rp_1 REAL,
                    Statbotics_red_rp_2 REAL,
                    Statbotics_blue_rp_2 REAL,
                    Statbotics_red_rp_3 REAL,
                    Statbotics_blue_rp_3 REAL
                );

                CREATE TABLE IF NOT EXISTS matches_depth_predictions (
                    match_key TEXT NOT NULL REFERENCES matches(Match),
                    team_key INT,
                    attr_name TEXT,
                    attr_value TEXT,
                    PRIMARY KEY (match_key, team_key, attr_name)
                );

                CREATE TABLE IF NOT EXISTS matches_fields (
                    match_key TEXT NOT NULL REFERENCES matches(Match),
                    team_key INT,
                    field_name TEXT,
                    field_value TEXT,
                    PRIMARY KEY (match_key, team_key, field_name)
                );

                CREATE TABLE IF NOT EXISTS teams (
                    Team INT PRIMARY KEY,
                    EPA REAL,
                    Rank REAL,
                    Average_RP REAL,
                    OPR REAL,
                    Last_OPR REAL,
                    Country TEXT,
                    State TEXT,
                    City TEXT,
                    Name TEXT,
                    School TEXT,
                    RookieYear INT,
                    PostalCode TEXT,
                    Website TEXT
                );

                CREATE TABLE IF NOT EXISTS teams_fields (
                    team_key INT NOT NULL REFERENCES teams(Team),
                    field_name TEXT,
                    field_value TEXT,
                    PRIMARY KEY (team_key, field_name)
                );
            """
            )
            conn.commit()

    def reset_fetch_timestamp(self):
        self.last_fetch_timestamp = time.time()

    def load_remote_data(self):
        try:
            self._tba_data = apputils.load_tba_data(
                self.event_key, self.tba_key, self.year
            )
            self._sb_epas = {
                s["team"]: round(s["epa"]["total_points"]["mean"], 1)
                for s in self.sb.get_team_events(
                    event="2026paca", limit=1000, fields=["team", "epa"]
                )
            }
            self._sb_matches = self.sb.get_matches(event=self.event_key)
        except Exception as e:
            logger.error(f"Error fetching remote data: {apputils.exception_format(e)}")

    def write_match_schedule_file(self):
        with sqlite3.connect(
            os.path.join(self.outpath, self.database_name)
        ) as conn:
            matches = []
            for match in self._tba_data.schedule:
                matches.append(
                    {
                        "Match": match["k"],
                    }
                    | {f"Red_{i + 1}": v for i, v in enumerate(match["r"])}
                    | {f"Blue_{i + 1}": v for i, v in enumerate(match["b"])}
                )
            all_fields = self.sql_fields["matches"]
            fields_not_writing_to = [
                x for x in all_fields if x not in matches[0].keys()
            ]
            for match in matches:
                conn.execute(
                    f"""
                    INSERT OR REPLACE INTO matches (Match, {", ".join(all_fields)}) VALUES (?, {", ".join(['?'] * len(all_fields))}) 
                """,
                    [
                        match["Match"],
                        match["Red_1"],
                        match["Red_2"],
                        match["Red_3"],
                        match["Blue_1"],
                        match["Blue_2"],
                        match["Blue_3"],
                    ]
                    + [None] * len(fields_not_writing_to),
                )
            conn.commit()

    def write_teams_file(self):
        with sqlite3.connect(
            os.path.join(self.outpath, self.database_name)
        ) as conn:
            all_fields = self.sql_fields["teams"]
            for team in self._tba_data.teams:
                data = []
                for field in all_fields:
                    if field in list(self._tba_data.team_info.values())[0].keys():
                        data.append(self._tba_data.team_info[team][field])
                    else:
                        data.append(None)
                conn.execute(
                    f"""
                    INSERT OR REPLACE INTO teams (Team, {", ".join(all_fields)}) VALUES (?, {", ".join(['?'] * len(all_fields))})
                """,
                    [team] + data,
                )
            conn.commit()

    def write_team_tba_data_file(self):
        df = []
        for team in self._tba_data.teams:
            rank, avg_rp = (
                self._tba_data.ranks[team]
                if team in self._tba_data.ranks
                else (None, None)
            )
            df.append(
                {
                    "Team": team,
                    "Rank": rank,
                    "Average_RP": avg_rp,
                    "OPR": (
                        self._tba_data.curr_oprs[team]
                        if team in self._tba_data.curr_oprs
                        else None
                    ),
                    "Last_OPR": (
                        self._tba_data.opr[team] if team in self._tba_data.opr else None
                    ),
                }
            )

        with sqlite3.connect(
            os.path.join(self.outpath, self.database_name)
        ) as conn:
            for team in df:
                conn.execute(
                    f"""
                    UPDATE teams
                    SET Rank = ?, Average_RP = ?, OPR = ?, Last_OPR = ?
                    WHERE Team = ?
                """,
                    (
                        team["Rank"],
                        team["Average_RP"],
                        team["OPR"],
                        team["Last_OPR"],
                        team["Team"],
                    ),
                )
            conn.commit()

    def write_team_fields(self):
        df = []
        for (
            k,
            v,
        ) in (
            self._teams.items()
        ):  # _teams is a dict with team: TeamStruct (ex. {422: TeamData()})
            # bind each team to the dict serialization of its cooresponding struct
            df.append({"Team": k} | v.output_dict(self.config_data))

        if len(df) <= 0:
            logger.warning("No team fields")
            return

        with sqlite3.connect(
            os.path.join(self.outpath, self.database_name)
        ) as conn:
            fields_to_write = list(df[0].keys())
            fields_to_write.remove("Team")
            for team in df:
                for field in fields_to_write:
                    conn.execute(
                        f"""
                        INSERT OR REPLACE INTO teams_fields (team_key, field_name, field_value) VALUES (?, ?, ?)
                    """,
                        (team["Team"], field, str(team[field])),
                    )

    def write_match_predictions_file(self):
        df = []
        for match in self._tba_data.schedule:
            score = [
                self.get_match_pred_score(match, "b"),
                self.get_match_pred_score(match, "r"),
            ]
            colors_pretty = ["Blue", "Red"]
            df.append(
                {
                    "Match": match["k"],
                }
                | reduce(
                    operator.or_,
                    [
                        {
                            f"Predict_{color.upper()}1_Score": round(score[i][0], 3),
                            f"Predict_{color.upper()}2_Score": round(score[i][1], 3),
                            f"Predict_{color.upper()}3_Score": round(score[i][2], 3),
                            f"Predict_{colors_pretty[i]}_Score": round(
                                sum(score[i]), 3
                            ),
                        }
                        for i, color in enumerate(["b", "r"])
                    ],
                )  # trust
                | {"Predict_Winner": "blue" if sum(score[0]) > sum(score[1]) else "red"}
            )

        with sqlite3.connect(
            os.path.join(self.outpath, self.database_name)
        ) as conn:
            fields_to_write = list(df[0].keys())
            fields_to_write.remove("Match")
            for match in df:
                conn.execute(
                    f"""
                    UPDATE matches
                    SET {", ".join([f"{x} = ?" for x in fields_to_write])}
                    WHERE Match = ?
                """,
                    [*(list(match.values())[1:]), match["Match"]],
                )
            conn.commit()

    def write_statbotics_analytics(self):
        df = []
        for match in self._sb_matches:
            df.append(
                {
                    "Match": match["key"],
                }
                | match["pred"]
            )
        with sqlite3.connect(
            os.path.join(self.outpath, self.database_name)
        ) as conn:
            fields_to_write = list(df[0].keys())
            fields_to_write.remove("Match")
            for match in df:
                conn.execute(
                    f"""
                    UPDATE matches
                    SET {", ".join([f"Statbotics_{x} = ?" for x in fields_to_write])}
                    WHERE Match = ?
                """,
                    [*(list(match.values())[1:]), match["Match"]],
                )
            conn.commit()

    def write_statbotics_epa(self):
        with sqlite3.connect(
            os.path.join(self.outpath, self.database_name)
        ) as conn:
            for team in self._tba_data.teams:
                conn.execute(
                    f"""
                    UPDATE teams
                    SET EPA = ?
                    WHERE Team = ?
                """,
                    (
                        self._sb_epas[team] if team in self._sb_epas else 0,
                        team,
                    ),
                )
            conn.commit()

    def write_match_fields(self):
        df = []
        for k, v in self._matches.items():
            for matTeam in v.output_dict(self.config_data):
                if int(k) <= len(self._tba_data.schedule):
                    df.append({"Match": self._tba_data.schedule[int(k) - 1]['k']} | matTeam)

        if len(df) <= 0:
            return

        with sqlite3.connect(
            os.path.join(self.outpath, self.database_name)
        ) as conn:
            fields_to_write = list(df[0].keys())
            fields_to_write.remove("Match")
            fields_to_write.remove("Team")
            for match in df:
                for field in fields_to_write:
                    conn.execute(
                        f"""
                        INSERT OR REPLACE INTO matches_fields (match_key, team_key, field_name, field_value) VALUES (?, ?, ?, ?)
                    """,
                        (match["Match"], match["Team"], field, str(match[field])),
                    )
            conn.commit()

    def write_match_depth_predictions(self):
        df = []
        for match in self._tba_data.schedule:
            for color in ["b", "r"]:
                for i in range(3):
                    team = match[color][i].removeprefix("frc")
                    dat = {
                        "Match": match["k"],
                        "Color": color,
                        "Team": team,
                        "OPR": self._tba_data.curr_oprs[int(team)],
                        "EPA": self._sb_epas[int(team)],
                    }
                    for copr in self.config_data["copr"]:
                        dat |= {copr: self._tba_data.copr[int(team)][copr]}
                    if int(team) in self._teams:
                        teamO = self._teams[int(team)].output_dict(self.config_data)
                        for field in self.config_data["deep-predict"]:
                            if field["source"] in teamO:
                                dat |= {
                                    field["name"]: teamO[field["source"]]
                                }  # append the relevant datas
                    else:
                        for field in self.config_data["deep-predict"]:
                            dat |= {field["name"]: 0}
                    df.append(dat)

        if len(df) <= 0:
            logger.warning("No depth predictions")
            return

        with sqlite3.connect(
            os.path.join(self.outpath, self.database_name)
        ) as conn:
            fields_to_write = list(df[0].keys())
            fields_to_write.remove("Match")
            fields_to_write.remove("Team")
            for match in df:
                for field in fields_to_write:
                    conn.execute(
                        f"""
                        INSERT OR REPLACE INTO matches_depth_predictions (match_key, team_key, attr_name, attr_value) VALUES (?, ?, ?, ?)
                    """,
                        (match["Match"], match["Team"], field, str(match[field])),
                    )
            conn.commit()

    def mad_filter(
        data, c=2
    ):  # https://real-statistics.com/sampling-distributions/identifying-outliers-missing-data
        """Filters the inputted `np.array` by removing all entries that are farther than c * MAD from the median"""
        median = np.median(data)  # X~
        diff = np.abs(data - median)  # diffs
        mad = np.median(diff)
        return data[diff <= (c * mad)]

    def round_sigfigs(x, sig=3):
        if np.isscalar(x):
            if x == 0.0 or np.isnan(np.log10(x)) or np.isinf(x):
                return x
            return np.around(x, sig - int(np.floor(np.log10(np.abs(x)))) - 1)
        else:
            return np.array([Processor.round_sigfigs(y) for y in x], dtype=np.float64)

    def get_svd_analysis(
        self, stat: pd.DataFrame, compteamname, tn_field
    ) -> tuple[dict[int, np.float64], dict[int, np.float64], np.float64]:
        """Returns the nomalized rank factors of each team, the variance score of each team, and the stability score"""
        # much thanks to pairwise
        arrLen = len(self._teams)
        matrix = np.zeros((arrLen, arrLen))
        teamkeys = list(self._teams.keys())
        team_index = {team: idx for idx, team in enumerate(teamkeys)}
        grouped = (
            stat.groupby([tn_field, compteamname])[stat.columns[2]].mean().fillna(0)
        )
        for (t1, t2), value in grouped.items():
            if t1 in team_index and t2 in team_index:
                i = team_index[t1]
                j = team_index[t2]
                matrix[i, j] = value
                matrix[j, i] = -value
        U, S, _ = svd(matrix)
        u_ranks: np.ndarray = U[:, 0]
        u_ranks = Processor.round_sigfigs(
            (u_ranks - u_ranks.min()) / (u_ranks.max() - u_ranks.min()) * 100
        )
        stability = S.max() / S.min() if S.min() > 0 else np.inf
        variation_score = np.zeros(len(S))  # less = more consistent
        for i in range(len(S)):
            variation_score[i] = Processor.round_sigfigs(
                np.sqrt(sum([(U[i][j] * S[j]) ** 2 for j in range(1, len(S))]))
            )

        return (
            dict(zip(teamkeys, u_ranks)),
            dict(zip(teamkeys, variation_score)),
            Processor.round_sigfigs(stability),
        )

    def get_percent_scouted(self) -> float:
        """gets the percentage of teams scouted in the current comp"""
        return round(
            len([x for x in self._teams.keys() if x in self._tba_data.teams])
            / len(self._tba_data.teams),
            2,  # use 2 because %
        )

    def get_team_epa(self, team):
        try:
            return self.sb.get_team_year(team, 2026)["epa"]["total_points"]["mean"]
        except:
            return 0

    def get_match_pred_score(self, match, c):
        """Gets the predicted match score based on the prediction-metric specified in the config yaml"""
        score = []
        for key in match[c]:
            this_score = 0
            source_string = self.config_data["p-metric"]["source"]
            if source_string == "OPR":
                this_score = self._tba_data.curr_oprs[int(key.removeprefix("frc"))]
            elif source_string == "Last_OPR":
                this_score = self._tba_data.opr[int(key.removeprefix("frc"))]
            elif source_string in self.config_data["copr"]:
                this_score = self._tba_data.copr[int(key.removeprefix("frc"))]
            elif source_string == "EPA":
                this_score = self._sb_epas[int(key.removeprefix("frc"))]
            elif int(key.removeprefix("frc")) in self._teams:
                this_score = self._teams[int(key.removeprefix("frc"))].output_dict(
                    self.config_data
                )[source_string]
            else:
                this_score = 0
            score.append(this_score)  # use prediction metric source
        return score

    def write_other_metrics(self, outfile) -> None:
        """writes the json other metrics (right now only percent teams scouted) to the outfile"""
        outfile = os.path.join(self.outpath, "other-metrics.json")
        with open(outfile, "w") as w:
            json.dump(
                {"Percent Teams Scouted": self.get_percent_scouted()}, w, indent=4
            )

    def delete_match_scouter(data_filepath: str, mn: str, si: str) -> None:
        df = pd.read_csv(data_filepath)
        df[~((df["MN"] == int(mn)) & (df["SI"] == si))].to_csv(
            data_filepath, index=False
        )

    def pre_filter_chunk(self, df: ObjectHolder[pd.DataFrame]):
        df.obj["Pre-filter-keep"] = True
        for i in range(len(self.config_data["pre-tests"])):
            logger.info(
                f"\tPerforming pre-test: {self.config_data["pre-tests"][i]["name"]} [{('x' * (i + 1)) + ('-' * (len(self.config_data["pre-tests"]) - (i + 1)))}]"
            )
            df.obj["Pre-filter-keep"] = (df.obj["Pre-filter-keep"]) & (
                eval_beakscript(
                    self.config_data["pre-tests"][i]["expr"],
                    df.obj,
                    "Data Test " + self.config_data["pre-tests"][i]["name"],
                )
            )
        df.obj = df.obj.loc[
            df.obj["Pre-filter-keep"] == True
        ]  # remove values that didn't pass the test (my English grade)
        df.obj.drop(columns=["Pre-filter-keep"], inplace=True)

    def pre_process_chunk(self, df: ObjectHolder[pd.DataFrame]):

        if len(self.config_data["preproc"]) > 0:

            def apply_preproc_row(row: pd.Series, prep) -> list[pd.Series]:
                new_rho = eval_beakscript(
                    prep["op"], row, "Preprocessor Function " + prep["name"]
                )
                if isinstance(new_rho, pd.Series):
                    if len(new_rho) > 0 and isinstance(new_rho.iloc[0], pd.Series):
                        return list(
                            new_rho
                        )  # if series of series, return list of series
                    return [
                        new_rho
                    ]  # else return 1 element list of series (the series)
                if isinstance(new_rho, (list, tuple)):
                    return list(new_rho)
                raise TypeError(
                    f"Error: unsupported return type for preproc function {prep["name"]}: {type(new_rho)}"
                )

            for i in range(len(self.config_data["preproc"])):
                last_cols = df.obj.columns
                logger.info(
                    f"\tPerforming preprocess operation: {self.config_data["preproc"][i]["name"]} [{('x' * (i + 1)) + ('-' * (len(self.config_data["preproc"]) - (i + 1)))}]"
                )
                exp = (
                    df.obj.apply(
                        lambda row: apply_preproc_row(
                            row, self.config_data["preproc"][i]
                        ),
                        axis=1,
                    )
                    .explode()
                    .reset_index(drop=True)
                )
                df.obj = pd.DataFrame(exp.tolist())
                if "new-headers" in self.config_data["preproc"][i]:
                    df.obj.columns = self.config_data["preproc"][i]["new-headers"]
                else:
                    df.obj.columns = last_cols

    def drop_duplicates_chunk(self, df: ObjectHolder[pd.DataFrame]):
        if len(self.config_data["uniques"]) > 0:
            dupes = df.obj.duplicated(subset=self.config_data["uniques"], keep=False)
            if dupes.any():
                logger.warning(
                    "Warning: duplicate teams for the following matches:\n\t\t"
                    + "\n\t\t".join(
                        str(
                            df.obj[dupes][self.config_data["uniques"]].drop_duplicates()
                        ).split("\n")
                    )
                )
                logger.info("\tFiltering out...")
            df.obj = df.obj.drop_duplicates(
                subset=self.config_data["uniques"], keep="first"
            )  # remove the duplicates

    def filter_chunk(self, df: ObjectHolder[pd.DataFrame]):
        df.obj["filter-keep"] = True
        for i in range(len(self.config_data["tests"])):
            logger.info(
                f"\tPerforming test: {self.config_data["tests"][i]["name"]} [{('x' * (i + 1)) + ('-' * (len(self.config_data["tests"]) - (i + 1)))}]"
            )
            df.obj["filter-keep"] = (df.obj["filter-keep"]) & (
                eval_beakscript(
                    self.config_data["tests"][i]["expr"],
                    df.obj,
                    "Data Test " + self.config_data["tests"][i]["name"],
                )
            )
        df.obj = df.obj.loc[
            df.obj["filter-keep"] == True
        ]  # remove values that didn't pass the test (my English grade)
        df.obj.drop(columns=["filter-keep"], inplace=True)

    def compute_fields_chunk(self, df: ObjectHolder[pd.DataFrame]):
        for comp in self.config_data["compute"]:
            # compute the beakscript formula with the current chunk (for each line), and output it into a new field named comp["name"]
            # this works because beakscript fully supports pd.DataFrame's, of which chunk is one
            df.obj[comp["name"]] = eval_beakscript(
                comp["eq"], df.obj, "Compute Field " + comp["name"]
            )

    def build_teams_chunk(self, df: ObjectHolder[pd.DataFrame]):
        for team in df.obj[self.config_data["tn"]].unique():
            self._teams |= {int(team): TeamStruct()}

    def svd_data_chunk(self, df: ObjectHolder[pd.DataFrame]):
        tn = self.config_data["tn"]
        for subj in self.config_data["svd"]:
            u, v, s = self.get_svd_analysis(
                df.obj[[self.config_data["tn"], subj["comp-team"], subj["source"]]],
                subj["comp-team"],
                tn,
            )
            svd_rank = []
            svd_var = []

            # make sorting
            ks = np.array(list(u.keys()))
            vs = np.array(list(u.values()))
            sorted_i = np.argsort(vs)
            sorted_v = vs[sorted_i]
            dense_ranks = np.zeros_like(vs, dtype=int)
            curr_rank = 0
            dense_ranks[sorted_i[0]] = curr_rank
            for i in range(1, len(vs)):
                if sorted_v[i] != sorted_v[i - 1]:
                    curr_rank += 1
                dense_ranks[sorted_i[i]] = curr_rank
            ranks = dict(zip(ks, dense_ranks))

            for team in df.obj[tn]:
                svd_rank.append(u[team])
                svd_var.append(v[team])
            df.obj[f"{subj["name"]}"] = svd_rank
            if "variance-score" in subj["augs"]:
                df.obj[f"{subj["name"]} Variance"] = svd_var
            if "stability" in subj["augs"]:
                df.obj[f"{subj["name"]} Stabillity"] = [
                    s for _ in range(len(df.obj[tn]))
                ]
            for team in df.obj[tn].unique():
                self._teams[int(team)].extend_data(
                    {
                        f"{subj["name"]}": ranks[team] + 1,
                        f"{subj["name"]} Variance": v[team],
                    }
                )

    def team_proc_chunk(self, df: ObjectHolder[pd.DataFrame]):
        for team in df.obj[
            self.config_data["tn"]
        ].unique():  # teams will be in the chunk multiple teams, but we just want to loop through all of the DIFFERENT teams there are
            team_data = {}
            for field in self.config_data["teams"]:
                # call the beakscript functions for the derived fields where the TN == team
                val = eval_beakscript(
                    field["derive"],
                    df.obj.loc[df.obj[self.config_data["tn"]] == team],
                    "Team Field " + field["name"],
                )
                if isinstance(val, pd.Series):
                    val = val.tolist()  # pythonify the pandas datatypes
                team_data[field["name"]] = val  # write the field to the csv
            self._teams[int(team)].extend_data(
                team_data
            )  # add the data to the appropriate team struct

    def match_proc_chunk(self, df: ObjectHolder[pd.DataFrame]):
        tn, mn = self.config_data["tn"], self.config_data["mn"]
        for match in df.obj[mn].unique():  # same thing as teams
            row = df.obj.loc[
                df.obj[mn] == match
            ]  # row is actually 6 rows up here to be used for static fields
            static_fields = {}
            for field in self.config_data[
                "matches"
            ]:  # parse static fields before breaking down match by team
                if "static" in field:
                    val = eval_beakscript(
                        field["derive"],
                        row,
                        "Static Match Field" + field["name"],
                    )
                    if isinstance(val, pd.Series):
                        val = val.tolist()

                    static_fields |= {field["name"]: val}
            for i, team in enumerate(
                df.obj.loc[df.obj[mn] == match, tn].unique()
            ):  # the .unique is uneccesary but safe
                data = {}
                for field in self.config_data["matches"]:
                    row = df.obj.loc[(df.obj[tn] == team) & (df.obj[mn] == match)]
                    if "static" in field:
                        continue
                    # get derived fields for matches
                    val = eval_beakscript(
                        field["derive"],
                        row.iloc[0],
                        "Match Field" + field["name"],
                    )
                    if isinstance(val, pd.Series):
                        val = val.tolist()
                    data[field["name"]] = val
                for f, fv in static_fields.items():
                    if isinstance(fv, Iterable) and not isinstance(fv, (str, bytes)):
                        data[f] = fv[i]
                    else:
                        data[f] = fv
                self._matches.setdefault(match, MatchStruct()).add_team_data(
                    int(team), data
                )

    def proccess_data(self, data_filepath: str) -> None:
        """Reads the input data, performs the calculations specified in field-config.yaml, and outputs all of the output files"""

        if ((time.time() - self.last_fetch_timestamp) / 60 >= self.period_min) or (
            self._tba_data.teams == None or self._tba_data.schedule == None
        ):
            self.periodic_calls.fire(logger.info)
            self.last_fetch_timestamp = time.time()

        if self._tba_data.teams == None or self._tba_data.schedule == None:
            raise Exception("Error: Missing TBA Key: Please add one in settings")

        self._teams.clear()
        self._matches.clear()

        with pd.read_csv(
            data_filepath,
            chunksize=self.chunk_size,
            iterator=True,
        ) as reader:
            first = True
            for chunk in reader:  # read in chunks in case big
                chunk_holder = ObjectHolder(chunk)
                self.chunk_processing_routine.fire(logger.info, chunk_holder)
                logger.info("Writing chunk...")
                # write the main csv
                chunk.to_csv(
                    os.path.join(self.outpath, self.outname),
                    index=False,
                    header=first,
                )
                first = False
        # write all the other files
        self.post_process_routine.fire(logger.info)
        logger.info("Done!!!!")
