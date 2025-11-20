import pandas as pd
import os
from itertools import chain
import json
import numpy as np
import warnings


class TeamStruct:
    def __init__(self):
        self.acoral = []
        self.aalgae = []
        self.tcoral = []
        self.talgae = []
        self.climbS = []
        self.climbD = []
        self.score = []
        self.defense = []
        self.acoral_filtered = np.array([])
        self.tcoral_filtered = np.array([])
        self.aalgae_filtered = np.array([])
        self.talgae_filtered = np.array([])
        self.score_filtered = np.array([])

    def extend_data(self, acoral, tcoral, aalge, talgae, climbS, climbD, score, defense):
        self.acoral = list(chain(self.acoral, acoral))
        self.tcoral = list(chain(self.tcoral, tcoral))
        self.aalgae = list(chain(self.aalgae, aalge))
        self.talgae = list(chain(self.talgae, talgae))
        self.climbS = list(chain(self.climbS, climbS))
        self.climbD = list(chain(self.climbD, climbD))
        self.score = list(chain(self.score, score))
        self.defense = defense

    def filter(self):
        self.acoral_filtered = Processor.mad_filter(np.array(self.acoral))
        self.tcoral_filtered = Processor.mad_filter(np.array(self.tcoral))
        self.aalgae_filtered = Processor.mad_filter(np.array(self.aalgae))
        self.talgae_filtered = Processor.mad_filter(np.array(self.talgae))
        self.score_filtered = Processor.mad_filter(np.array(self.score))
        max_len_auto = max(len(self.acoral_filtered), len(self.aalgae_filtered))
        self.acoral_filtered = np.pad(self.acoral_filtered, (0, max_len_auto - len(self.acoral_filtered)), constant_values=0)
        self.aalgae_filtered = np.pad(self.aalgae_filtered, (0, max_len_auto - len(self.aalgae_filtered)), constant_values=0)

        max_len_tele= max(len(self.tcoral_filtered), len(self.talgae_filtered))
        self.tcoral_filtered = np.pad(self.tcoral_filtered, (0, max_len_tele - len(self.tcoral_filtered)), constant_values=0)
        self.talgae_filtered = np.pad(self.talgae_filtered, (0, max_len_tele - len(self.talgae_filtered)), constant_values=0)

    def get_avg_auto(self):
        return round((sum(self.acoral) + sum(self.aalgae)) / max(len(self.aalgae), len(self.acoral)), 2)
    
    def get_max_auto(self):
        return max(a + c for a, c in zip(self.aalgae, self.acoral))
    
    def get_avg_auto_fil(self):
        return float(np.around(np.mean(self.aalgae_filtered + self.acoral_filtered), 2))
    
    def get_avg_tele(self):
        return round((sum(self.tcoral) + sum(self.talgae)) / max(len(self.talgae), len(self.tcoral)), 2)
    
    def get_max_tele(self):
        return max(a + c for a, c in zip(self.talgae, self.tcoral))
    
    def get_avg_tele_fil(self):
        return float(np.around(np.mean(self.talgae_filtered + self.tcoral_filtered), 2))
    
    def get_max_total(self):
        return max(aa + ac + ta + tc for aa, ac, ta, tc in zip(self.aalgae, self.acoral, self.talgae, self.tcoral)) # need to recompute bc totmax != amax + tmax

    def get_avg_aalg(self):
        return round(sum(self.aalgae) / len(self.aalgae), 2)
    
    def get_max_aalg(self):
        return max(self.aalgae)
    
    def get_avg_aalg_fil(self):
        return float(np.around(np.mean(self.aalgae_filtered), 2))
        
    def get_avg_talg(self):
        return round(sum(self.talgae) / len(self.talgae), 2)
    
    def get_max_talg(self):
        return max(self.talgae)
    
    def get_avg_talg_fil(self):
        return float(np.around(np.mean(self.talgae_filtered), 2))
    
    def get_max_totalg(self):
        return max(aa + ta for aa, ta in zip(self.aalgae, self.talgae))

    def get_avg_acoral(self):
        return round(sum(self.acoral) / len(self.acoral), 2)
    
    def get_max_acoral(self):
        return max(self.acoral)

    def get_avg_acoral_fil(self):
        return float(np.around(np.mean(self.acoral_filtered), 2))
    
    def get_avg_tcoral(self):
        return round(sum(self.tcoral) / len(self.tcoral), 2)
    
    def get_max_tcoral(self):
        return max(self.tcoral)

    def get_avg_tcoral_fil(self):
        return float(np.around(np.mean(self.tcoral_filtered), 2))
    
    def get_max_totcoral(self):
        return max(ac + tc for ac, tc in zip(self.acoral, self.tcoral))

    def get_avg_climbS(self):
        return round(sum(self.climbS) / len(self.climbS), 2)

    def get_avg_climbD(self):
        return round(sum(self.climbD) / len(self.climbD), 2)

    def get_avg_score(self):
        return round(sum(self.score) / len(self.score))
    
    def get_max_score(self):
        return max(self.score)
    
    def get_avg_score_fil(self):
        return float(np.around(np.mean(self.score_filtered), 2))
    
    def get_avg_def(self):
        return round(sum(self.defense) / len(self.defense), 2) if len(self.defense) > 0 else "N/A"
    
    def get_max_def(self):
        return max(self.defense) if len(self.defense) > 0 else "N/A"
    
    def get_def_exp(self):
        return len(self.defense) if len(self.defense) > 0 else 0


class MatchStruct:
    def __init__(self):
        self.teams = {}

    def add_team_cycles(self, team, acoral, tcoral, aalgae, talgae, atotal, ttotal, score, climbP, climbS, climbD):
        t = self.teams.setdefault(team, {})
        t["acoral"] = acoral
        t["tcoral"] = tcoral
        t["aalgae"] = aalgae
        t["talgae"] = talgae
        t["atotal"] = atotal
        t["ttotal"] = ttotal
        t["score"] = score
        t["climbP"] = climbP
        t["climbS"] = climbS
        t["climbD"] = climbD


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

    def __init__(self, outpath, chunk_size, teams, sched):
        self.chunk_size = chunk_size
        self.outpath = outpath
        self._teamsAt = teams
        self._sched = sched
        self._teams: dict[str, TeamStruct] = {}
        self._matches = {}

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
            df.append({
                "Team": k,
                "Average Algae Auton": v.get_avg_aalg(),
                "Max Algae Auton": v.get_max_aalg(),
                "Filtered Algae Auton": v.get_avg_aalg_fil(),
                "Average Algae Teleop": v.get_avg_talg(),
                "Max Algae Teleop": v.get_max_talg(),
                "Filtered Algae Teleop": v.get_avg_talg_fil(),
                "Max Total Algae": v.get_max_totalg(), # hard to compute in grafana bc. max is not linear (max(a + b) != max(a) + max(b)) with elementwise +
                "Average Coral Auton": v.get_avg_acoral(),
                "Max Coral Auton": v.get_max_acoral(),
                "Filtered Coral Auton": v.get_avg_acoral_fil(),
                "Average Coral Teleop": v.get_avg_tcoral(),
                "Max Coral Teleop": v.get_max_tcoral(),
                "Filtered Coral Teleop": v.get_avg_tcoral_fil(),
                "Max Total Coral": v.get_max_totcoral(),
                "Average Cycles Auton": v.get_avg_auto(),
                "Max Cycles Auton": v.get_max_auto(),
                "Filtered Cycles Auton": v.get_avg_auto_fil(),
                "Average Cycles Teleop": v.get_avg_tele(),
                "Max Cycles Teleop": v.get_max_tele(),
                "Filtered Cycles Teleop": v.get_avg_tele_fil(),
                "Max Total Cycles": v.get_max_total(),
                "Average Shallow": v.get_avg_climbS(),
                "Average Deep": v.get_avg_climbD(),
                "Average Score": v.get_avg_score(),
                "Max Score": v.get_max_score(),
                "Filtered Score": v.get_avg_score_fil(),
                "Average Defense Rating": v.get_avg_def(),
                "Max Defense Rating": v.get_max_def(),
                "Matches Playing Defense": v.get_def_exp(),
            })
        pd.DataFrame(df).to_csv(outfile, index=False)

    def get_match_pred_score(self, match, c):
        score = []
        for key in match[c]:
            if int(key.removeprefix("frc")) in self._teams:
                score.append(self._teams[
                    int(key.removeprefix("frc"))
                ].get_avg_score_fil())

        return score
    
    def match_predict_depth(self, outfile):
        df = []
        for match in self._sched:
            for color in ['b', 'r']:
                for i in range(3):
                    team = match[color][i].removeprefix("frc")
                    teamO = self._teams[int(team)]
                    df.append({
                        "Match": match['k'],
                        "Color": color,
                        "Team": team,
                        "Shallow Climb": teamO.get_avg_climbS(),
                        "Deep Climb": teamO.get_avg_climbD(),
                        "Auto Cycles": teamO.get_avg_auto(),
                        "Teleop Cycles": teamO.get_avg_tele(),
                        "Def": teamO.get_avg_def(),
                    })
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
            for tk, tv in v.teams.items():
                df.append({
                    "Match": k,
                    "Team": tk,
                    "Total Coral Auton": int(tv["acoral"]),
                    "Total Coral Teleop": int(tv["tcoral"]),
                    "Total Algae Auton": int(tv["aalgae"]),
                    "Total Algae Teleop": int(tv["talgae"]),
                    "Total Cycles Auton": int(tv["atotal"]),
                    "Total Cycles Teleop": int(tv["ttotal"]),
                    "Score": int(tv["score"]),
                    "Park": int(tv["climbP"]),
                    "Shallow Climb": int(tv["climbS"]),
                    "Deep Climb": int(tv["climbD"])
                })
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
                chunk["Total ACoral"] = chunk[Processor.CORAL_COLUMNS_A].sum(axis=1)
                chunk["Total TCoral"] = chunk[Processor.CORAL_COLUMNS_T].sum(axis=1)
                chunk["Total AAlgae"] = chunk[Processor.ALGAE_COLUMNS_A].sum(axis=1)
                chunk["Total TAlgae"] = chunk[Processor.ALGAE_COLUMNS_T].sum(axis=1)
                chunk["Total ACycles"] = chunk["Total ACoral"] + chunk["Total AAlgae"]
                chunk["Total TCycles"] = chunk["Total TCoral"] + chunk["Total TAlgae"]
                chunk["Total Score"] = (
                    (chunk[Processor.CORAL_COLUMNS_A] * Processor.CORAL_VALS_A).sum(axis=1)
                    + (chunk[Processor.CORAL_COLUMNS_T] * Processor.CORAL_VALS_T).sum(axis=1)
                    + (chunk[Processor.ALGAE_COLUMNS_A] * Processor.ALGAE_VALS).sum(axis=1)
                    + (chunk[Processor.ALGAE_COLUMNS_T] * Processor.ALGAE_VALS).sum(axis=1)
                    + chunk["M"] * 3
                    + chunk["CP"] * 2
                    + chunk["CS"] * 6
                    + chunk["CD"] * 12
                )
                for team in chunk["TN"]:
                    self._teams.setdefault(team, TeamStruct()).extend_data(
                        chunk.loc[chunk["TN"] == team, "Total ACoral"],
                        chunk.loc[chunk["TN"] == team, "Total TCoral"],
                        chunk.loc[chunk["TN"] == team, "Total AAlgae"],
                        chunk.loc[chunk["TN"] == team, "Total TAlgae"],
                        chunk.loc[chunk["TN"] == team, "CS"],
                        chunk.loc[chunk["TN"] == team, "CD"],
                        chunk.loc[chunk["TN"] == team, "Total Score"],
                        chunk.loc[(chunk["TN"] == team) & (chunk["D"] > 0), "DR"],
                    )
                for match in chunk["MN"]:
                    for team in chunk.loc[
                        chunk["MN"] == match, "TN"
                    ]:
                        row = chunk.loc[
                            (chunk["TN"] == team)
                            & (chunk["MN"] == match)
                        ]
                        self._matches.setdefault(match, MatchStruct()).add_team_cycles(
                            team,
                            row.at[row.index[0], "Total ACoral"],
                            row.at[row.index[0], "Total TCoral"],
                            row.at[row.index[0], "Total AAlgae"],
                            row.at[row.index[0], "Total TAlgae"],
                            row.at[row.index[0], "Total ACycles"],
                            row.at[row.index[0], "Total TCycles"],
                            row.at[row.index[0], "Total Score"],
                            row.at[row.index[0], "CP"],
                            row.at[row.index[0], "CS"],
                            row.at[row.index[0], "CD"],
                        )
                chunk.to_csv(
                    os.path.join(self.outpath, outname),
                    index=False,
                    header=first,
                )
                first = False
        for v in self._teams.values(): v.filter()
        self.output_teams(os.path.join(self.outpath, outname + "-teams.csv"))
        self.output_matches(os.path.join(self.outpath, outname + "-matches.csv"))
        self.predict_matches(os.path.join(self.outpath, outname + "-predict.csv"))
        self.match_predict_depth(os.path.join(self.outpath, outname + "-morepredict.csv"))
        self.write_other_metrics(os.path.join(self.outpath, "other-metrics.json"))
