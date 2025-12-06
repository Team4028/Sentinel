import pandas as pd
import os
import json
import numpy as np
import warnings
try: 
    from lib.data_config import eval_beakscript
except ModuleNotFoundError:
    from src.lib.data_config import eval_beakscript
from collections import defaultdict

class TeamStruct:
    def __init__(self):
        self.data = {}
    def extend_data(self, data):
        merged = defaultdict(list)
        for d in [self.data, data]:
            for k, v in d.items():
                try:
                    merged[k].extend(v)
                except:
                    merged[k].append(v)
        self.data = dict(merged)

    def output_dict(self, config):
        data = {}
        for field in config["teams"]: data |= DataField(field["name"], self.data[field["name"]], field["filters"]).objectify()
        return data

FANCY_FIL = {"avg": "Average", "max": "Max", "fil": "Filtered"}
    
class DataField:
    filters = []
    data = []
    name = ""
    def __init__(self, name, data, filters):
        self.name = name
        self.filters = filters
        self.data = data

    def average(self):
        return round(sum(self.data) / len(self.data), 2) if len(self.data) > 0 else "N/A"
    def max(self):
        return max(self.data) if len(self.data) > 0 else "N/A"
    def filter(self):
        return float(np.around(np.mean(Processor.mad_filter(np.array(self.data))), 2))
    
    calc_map = {"avg": average, "max": max, "fil": filter}
        
    def objectify(self):
        data = {}
        if (len(self.filters) > 0):
            for fil in self.filters:
                data[FANCY_FIL[fil] + " " + self.name] = self.calc_map[fil](self)
        elif type(self.data) == list and len(self.data) == 1:
            data[self.name] = self.data[0]
        else: data[self.name] = self.data
        return data

class MatchStruct:
    def __init__(self):
        self.teams = {}

    def add_team_data(self, team, data):
        self.teams.setdefault(team, data)

    def output_dict(self, config):
        data = []
        for team in self.teams.keys():
            d2 = {"Team": team}
            for field in config["matches"]:
                d2 |= DataField(field["name"], self.teams[team][field["name"]], field["filters"]).objectify()
            data.append(d2)
        return data        


class Processor:
    CORAL_COLUMNS_A = [
        "AL1",
        "AL2",
        "AL3",
        "AL4",
    ]
    CORAL_COLUMNS_T = [
        "TL1",
        "TL2",
        "TL3",
        "TL4",
    ]
    ALGAE_COLUMNS_A = ["ATP", "ATB"]
    ALGAE_COLUMNS_T = ["AP", "AB"]
    CORAL_VALS_A = [3, 4, 6, 7]
    CORAL_VALS_T = [2, 3, 4, 5]
    ALGAE_VALS = [2, 4]

    def __init__(self, outpath, chunk_size, teams, sched, config_data):
        self.chunk_size = chunk_size
        self.outpath = outpath
        self._teamsAt = teams
        self._sched = sched
        self._teams: dict[str, TeamStruct] = {}
        self._matches = {}
        self.config_data = config_data

    def mad_filter(data, c=2): #https://real-statistics.com/sampling-distributions/identifying-outliers-missing-data
        median = np.median(data) # X~
        diff = np.abs(data - median) # diffs
        mad = np.median(diff)
        return data[diff <= (c * mad)]
    
    def get_percent_scouted(self):
        return round(len([x for x in self._teams.keys() if x in self._teamsAt]) / len(self._teamsAt), 2)

    def output_teams(self, outfile):
        df = []
        for k, v in self._teams.items():
            df.append({"Team": k} | v.output_dict(self.config_data))
        pd.DataFrame(df).to_csv(outfile, index=False)

    def get_match_pred_score(self, match, c):
        score = []
        for key in match[c]:
            if int(key.removeprefix("frc")) in self._teams:
                score.append(self._teams[
                    int(key.removeprefix("frc"))
                ].output_dict(self.config_data)[self.config_data["p-metric"]["source"]])

        return score
    
    def match_predict_depth(self, outfile):
        df = []
        for match in self._sched:
            for color in ['b', 'r']:
                for i in range(3):
                    team = match[color][i].removeprefix("frc")
                    teamO = self._teams[int(team)].output_dict(self.config_data)
                    dat = {
                        "Match": match['k'],
                        "Color": color,
                        "Team": team,
                    }
                    for field in self.config_data["deep-predict"]:
                        dat |= {field["name"]: teamO[field["source"]]}
                    df.append(dat)
        pd.DataFrame(df).to_csv(outfile, index=False)

    def predict_matches(self, outfile):
        df = []
        for match in self._sched:
            for color in ['b', 'r']:
                score = self.get_match_pred_score(match, color)
                df.append({
                    "Match": match['k'],
                    "Teams": " + ".join(map(lambda x: x.removeprefix("frc"),  match[color])) + f" ({"Blue" if color == "b" else "Red"})",
                    "1 Score": round(score[0]),
                    "2 Score": round(score[1]),
                    "3 Score": round(score[2]),
                    "Score": round(sum(score)),
                    "Won": 1 if (sum(score) > sum(self.get_match_pred_score(match, 'b' if color == 'r' else 'r'))) else float("nan")
                })
        pd.DataFrame(df).to_csv(outfile, index=False)

    def write_other_metrics(self, outfile):
        with open(outfile, "w") as w:
            json.dump({
                "Percent Teams Scouted": self.get_percent_scouted()
            }, w)

    def output_matches(self, outfile):
        df = []
        for k, v in self._matches.items():
            for matTeam in v.output_dict(self.config_data):
                df.append({
                    "Match": k
                } | matTeam)
        pd.DataFrame(df).to_csv(outfile, index=False)

    def proccess_data(self, data_filepath: str, outname):
        with pd.read_csv(
            data_filepath, chunksize=self.chunk_size, iterator=True
        ) as reader:
            first = True
            for chunk in reader:
                dupes = chunk.duplicated(subset=["MN", "TN"], keep=False)
                if dupes.any():
                    warnings.warn("Warning: duplicate teams for the following matches:")
                    print(chunk[dupes][["MN", "TN"]].drop_duplicates())
                    print("Filtering out...")
                chunk = chunk.drop_duplicates(subset=["MN", "TN"], keep="first")
                for comp in self.config_data["compute"]:
                    chunk[comp["name"]] = eval_beakscript(comp["eq"], chunk)
                for team in chunk["TN"].unique():
                    team_data = {}
                    for field in self.config_data["teams"]:
                        val = eval_beakscript(field["derive"], chunk.loc[chunk["TN"] == team])
                        if type(val) == pd.Series:
                            val = val.tolist()
                        team_data[field["name"]] = val
                    self._teams.setdefault(int(team), TeamStruct()).extend_data(team_data)
                for match in chunk["MN"]:
                    for team in chunk.loc[chunk["MN"] == match, "TN"]:
                        row = chunk.loc[(chunk["TN"] == team) & (chunk["MN"] == match)]
                        data = {}
                        for field in self.config_data["matches"]:
                            val = eval_beakscript(field["derive"], row.iloc[0])
                            if type(val) == pd.Series:
                                val = val.tolist()
                            data[field["name"]] = val
                        self._matches.setdefault(match, MatchStruct()).add_team_data(
                            int(team),
                            data
                        )
                chunk.to_csv(
                    os.path.join(self.outpath, outname),
                    index=False,
                    header=first,
                )
                first = False
        self.output_teams(os.path.join(self.outpath, outname + "-teams.csv"))
        self.output_matches(os.path.join(self.outpath, outname + "-matches.csv"))
        self.predict_matches(os.path.join(self.outpath, outname + "-predict.csv"))
        self.match_predict_depth(os.path.join(self.outpath, outname + "-morepredict.csv"))
        self.write_other_metrics(os.path.join(self.outpath, "other-metrics.json"))
