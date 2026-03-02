import math
import pandas as pd
import os
import json
import numpy as np

try:
    from lib.data_config import eval_beakscript, FANCY_FIL
except ModuleNotFoundError:
    from src.lib.data_config import eval_beakscript, FANCY_FIL
from collections import defaultdict
from collections.abc import Iterable
import logging
from scipy.linalg import svd

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

    def output_dict(self, config, _opr, _curr_opr):
        """Get all of the team data as a dictionary, unwrapping DataFields to get averages and such"""
        data = {
            "Rank": self.data["Rank"][0],
            "Average RP": self.data["Average RP"][0],
            "OPR": _curr_opr,
            "Last OPR": _opr,
        }
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


class Processor:
    """Main class of data calculation, handles all calculation basically"""

    def __init__(
        self, outpath, chunk_size, teams, sched, ranks, oprs, curr_oprs, config_data
    ) -> None:
        self.chunk_size = chunk_size
        self.outpath = outpath
        self._teamsAt = teams
        self._sched = sched
        self._teams: dict[str, TeamStruct] = {}
        self._matches = {}
        self._ranks = ranks
        self._oprs = oprs
        self._curr_oprs = curr_oprs
        self.config_data = config_data

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
            if x == 0.0 or np.isnan(np.log10(x)):
                return x
            return np.around(x, sig - int(np.floor(np.log10(np.abs(x)))) - 1)
        else:
            return np.array([Processor.round_sigfigs(y) for y in x], dtype=np.float64)

    def get_svd_analysis(
        self, stat: pd.DataFrame, compteamname
    ) -> tuple[dict[int, np.float64], dict[int, np.float64], np.float64]:
        """Returns the nomalized rank factors of each team, the variance score of each team, and the stability score"""
        # much thanks to pairwise
        arrLen = len(self._teams)
        matrix = np.zeros((arrLen, arrLen))
        teamkeys = list(self._teams.keys())
        team_index = {team: idx for idx, team in enumerate(teamkeys)}
        grouped = (stat.groupby(["TN", compteamname])[stat.columns[2]].mean())
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
        stability = S.max() / S.min() if S.min() > 1e-12 else np.inf
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
            len([x for x in self._teams.keys() if x in self._teamsAt])
            / len(self._teamsAt),
            2,  # use 2 because %
        )

    def output_teams(self, outfile) -> None:
        """writes all of the calculated team data (averages, etc.) to the outfile"""
        df = []
        for (
            k,
            v,
        ) in (
            self._teams.items()
        ):  # _teams is a dict with team: TeamStruct (ex. {422: TeamData()})
            # bind each team to the dict serialization of its cooresponding struct
            df.append(
                {"Team": k}
                | v.output_dict(
                    self.config_data, self._oprs[str(k)] if str(k) in self._oprs else 0, self._curr_oprs[str(k)] if str(k) in self._curr_oprs else 0
                )
            )
        pd.DataFrame(df).to_csv(outfile, index=False)

    def get_match_pred_score(self, match, c):
        """Gets the predicted match score based on the prediction-metric specified in the config yaml"""
        score = []
        for key in match[c]:
            if int(key.removeprefix("frc")) in self._teams:
                score.append(
                    self._teams[  # use the TeamStruct -> dict to get the data
                        int(key.removeprefix("frc"))
                    ].output_dict(
                        self.config_data,
                        self._oprs[key.removeprefix("frc")] if key.removeprefix("frc") in self._oprs else 0,
                        self._curr_oprs[key.removeprefix("frc")] if key.removeprefix("frc") in self._curr_oprs else 0,
                    )[
                        self.config_data["p-metric"]["source"]
                    ]
                )  # use prediction metric source
            else:
                score.append(0)
        return score

    def match_predict_depth(self, outfile) -> None:
        """Write in-depth match prediction (things like deep climb and cycles, info about the teams playing) to outfile"""
        df = []
        for match in self._sched:
            for color in ["b", "r"]:
                for i in range(3):
                    team = match[color][i].removeprefix("frc")
                    dat = {
                        "Match": match["k"],
                        "Color": color,
                        "Team": team,
                    }
                    if int(team) in self._teams:
                        teamO = self._teams[int(team)].output_dict(
                            self.config_data, self._oprs[team] if team in self._oprs else 0, self._curr_oprs[team] if team in self._curr_oprs else 0
                        )
                        for field in self.config_data["deep-predict"]:
                            dat |= {
                                field["name"]: teamO[field["source"]]
                            }  # append the relevant datas
                    else:
                        for field in self.config_data["deep-predict"]:
                            dat |= {field["name"]: 0}
                    df.append(dat)
        pd.DataFrame(df).to_csv(outfile, index=False)

    def predict_matches(self, outfile) -> None:
        """Predicts the score contributions, overall score, and winner of each match, writing to outfile"""
        df = []
        for match in self._sched:
            for color in ["b", "r"]:
                score = self.get_match_pred_score(match, color)
                df.append(
                    {
                        "Match": match["k"],
                        "Teams": " + ".join(
                            map(lambda x: x.removeprefix("frc"), match[color])
                        )
                        + f" ({"Blue" if color == "b" else "Red"})",
                        "1 Score": round(score[0]),
                        "2 Score": round(score[1]),
                        "3 Score": round(score[2]),
                        "Score": round(sum(score)),
                        # kind of weird because of grafana hackery, uses nan to better convert to a grafana boolean.
                        # there are seperate df entries for each color, and so this will be 1 if this color won and nan if they lost
                        "Won": (
                            1
                            if (
                                sum(score)
                                > sum(
                                    self.get_match_pred_score(
                                        match, "b" if color == "r" else "r"
                                    )
                                )
                            )
                            else float("nan")
                        ),
                    }
                )
        pd.DataFrame(df).to_csv(outfile, index=False)

    def write_other_metrics(self, outfile) -> None:
        """writes the json other metrics (right now only percent teams scouted) to the outfile"""
        with open(outfile, "w") as w:
            json.dump(
                {"Percent Teams Scouted": self.get_percent_scouted()}, w, indent=4
            )

    def output_matches(self, outfile) -> None:
        """outputs the match data (what each team in each match did) to the outfile"""
        df = []
        for k, v in self._matches.items():
            for matTeam in v.output_dict(self.config_data):
                df.append(
                    {
                        "Match": k
                        # '|' combines the dicts
                    }
                    | matTeam
                )
        pd.DataFrame(df).to_csv(outfile, index=False)

    def delete_match_team(data_filepath: str, mn: str, tn: str) -> None:
        df = pd.read_csv(data_filepath)
        df[~((df["MN"] == int(mn)) & (df["TN"] == int(tn)))].to_csv(
            data_filepath, index=False
        )

    def proccess_data(self, data_filepath: str, outname) -> None:
        """Reads the input data, performs the calculations specified in field-config.yaml, and outputs all of the output files"""

        if self._teamsAt == None or self._sched == None:
            raise Exception("Error: Missing TBA Key: Please add one in settings")

        self._teams.clear()
        self._matches.clear()

        with pd.read_csv(
            data_filepath, chunksize=self.chunk_size, iterator=True
        ) as reader:
            first = True
            tn, mn = self.config_data["tn"], self.config_data["mn"]
            for chunk in reader:  # read in chunks in case big
                if len(self.config_data["preproc"]) > 0:

                    def apply_preproc_row(row: pd.Series, prep) -> list[pd.Series]:
                        new_rho = eval_beakscript(
                            prep["op"], row, "Preprocessor Function " + prep["name"]
                        )
                        if isinstance(new_rho, pd.Series):
                            if len(new_rho) > 0 and isinstance(
                                new_rho.iloc[0], pd.Series
                            ):
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
                        last_cols = chunk.columns
                        logger.info(
                            f"Performing preprocess operation: {self.config_data["preproc"][i]["name"]} [{('x' * (i + 1)) + ('-' * (len(self.config_data["preproc"]) - (i + 1)))}]"
                        )
                        exp = (
                            chunk.apply(
                                lambda row: apply_preproc_row(
                                    row, self.config_data["preproc"][i]
                                ),
                                axis=1,
                            )
                            .explode()
                            .reset_index(drop=True)
                        )
                        chunk = pd.DataFrame(exp.tolist())
                        if "new-headers" in self.config_data["preproc"][i]:
                            chunk.columns = self.config_data["preproc"][i][
                                "new-headers"
                            ]
                        else:
                            chunk.columns = last_cols
                # if MN and TN are the same, there are two instances of the same team in the same match, so it's a duplicate row
                if len(self.config_data["uniques"]) > 0:
                    dupes = chunk.duplicated(
                        subset=self.config_data["uniques"], keep=False
                    )
                    if dupes.any():
                        logger.warning(
                            "Warning: duplicate teams for the following matches:\n\t"
                            + "\n\t".join(
                                str(
                                    chunk[dupes][
                                        self.config_data["uniques"]
                                    ].drop_duplicates()
                                ).split("\n")
                            )
                        )
                        logger.info("Filtering out...")
                    chunk = chunk.drop_duplicates(
                        subset=self.config_data["uniques"], keep="first"
                    )  # remove the duplicates
                chunk["filter-keep"] = True
                for i in range(len(self.config_data["tests"])):
                    logger.info(
                        f"Performing test: {self.config_data["tests"][i]["name"]} [{('x' * (i + 1)) + ('-' * (len(self.config_data["tests"]) - (i + 1)))}]"
                    )
                    chunk["filter-keep"] = (chunk["filter-keep"]) & (
                        eval_beakscript(
                            self.config_data["tests"][i]["expr"],
                            chunk,
                            "Data Test " + self.config_data["tests"][i]["name"],
                        )
                    )
                chunk = chunk.loc[
                    chunk["filter-keep"] == True
                ]  # remove values that didn't pass the test (my English grade)
                chunk.drop(columns=["filter-keep"], inplace=True)
                for comp in self.config_data["compute"]:
                    # compute the beakscript formula with the current chunk (for each line), and output it into a new field named comp["name"]
                    # this works because beakscript fully supports pd.DataFrame's, of which chunk is one
                    chunk[comp["name"]] = eval_beakscript(
                        comp["eq"], chunk, "Compute Field " + comp["name"]
                    )
                for team in chunk[tn].unique():
                    self._teams |= {int(team): TeamStruct()}
                for subj in self.config_data["svd"]:
                    u, v, s = self.get_svd_analysis(
                        chunk[[tn, subj["comp-team"], subj["source"]]],
                        subj["comp-team"],
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

                    for team in chunk[tn]:
                        svd_rank.append(u[team])
                        svd_var.append(v[team])
                    chunk[f"{subj["name"]}"] = svd_rank
                    if "variance-score" in subj["augs"]:
                        chunk[f"{subj["name"]} Variance"] = svd_var
                    if "stability" in subj["augs"]:
                        chunk[f"{subj["name"]} Stabillity"] = [
                            s for _ in range(len(chunk[tn]))
                        ]
                    for team in chunk[tn].unique():
                        self._teams[int(team)].extend_data(
                            {
                                f"{subj["name"]}": ranks[team] + 1,
                                f"{subj["name"]} Variance": v[team],
                            }
                        )

                for team in chunk[
                    tn
                ].unique():  # teams will be in the chunk multiple teams, but we just want to loop through all of the DIFFERENT teams there are
                    team_data = {}
                    if int(team) in self._ranks:
                        team_data["Rank"], team_data["Average RP"] = self._ranks[
                            int(team)
                        ]
                    else:
                        team_data["Rank"], team_data["Average RP"] = (None, None)
                    for field in self.config_data["teams"]:
                        # call the beakscript functions for the derived fields where the TN == team
                        val = eval_beakscript(
                            field["derive"],
                            chunk.loc[chunk[tn] == team],
                            "Team Field " + field["name"],
                        )
                        if isinstance(val, pd.Series):
                            val = val.tolist()  # pythonify the pandas datatypes
                        team_data[field["name"]] = val  # write the field to the csv
                    self._teams[int(team)].extend_data(
                        team_data
                    )  # add the data to the appropriate team struct
                for match in chunk[mn].unique():  # same thing as teams
                    row = chunk.loc[
                        chunk[mn] == match
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
                        chunk.loc[chunk[mn] == match, tn].unique()
                    ):  # the .unique is uneccesary but safe
                        data = {}
                        for field in self.config_data["matches"]:
                            row = chunk.loc[(chunk[tn] == team) & (chunk[mn] == match)]
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
                            if isinstance(fv, Iterable) and not isinstance(
                                fv, (str, bytes)
                            ):
                                data[f] = fv[i]
                            else:
                                data[f] = fv
                        self._matches.setdefault(match, MatchStruct()).add_team_data(
                            int(team), data
                        )
                logger.info("Writing chunk...")
                # write the main csv
                chunk.to_csv(
                    os.path.join(self.outpath, outname),
                    index=False,
                    header=first,
                )
                first = False
        # write all the other files
        logger.info("Writing teams...")
        self.output_teams(os.path.join(self.outpath, outname + "-teams.csv"))
        logger.info("Writing matches...")
        self.output_matches(os.path.join(self.outpath, outname + "-matches.csv"))
        logger.info("Writing predictions...")
        self.predict_matches(os.path.join(self.outpath, outname + "-predict.csv"))
        logger.info("Writing deep predictions...")
        self.match_predict_depth(
            os.path.join(self.outpath, outname + "-morepredict.csv")
        )
        logger.info("Writing other metrics...")
        self.write_other_metrics(os.path.join(self.outpath, "other-metrics.json"))
        logger.info("Done!!!!")
