import pandas as pd
import os
from itertools import chain
import json


class TeamStruct:
    def __init__(self):
        self.coral = []
        self.algae = []
        self.climbS = []
        self.climbD = []
        self.score = []

    def extend_data(self, coral, algae, climbS, climbD, score):
        self.coral = list(chain(self.coral, coral))
        self.algae = list(chain(self.algae, algae))
        self.climbS = list(chain(self.climbS, climbS))
        self.climbD = list(chain(self.climbD, climbD))
        self.score = list(chain(self.score, score))

    def get_avg_total(self):
        return round(
            (sum(self.coral) + sum(self.algae)) / max(len(self.algae), len(self.coral)),
            2,
        )

    def get_avg_alg(self):
        return round(sum(self.algae) / len(self.algae), 2)

    def get_avg_coral(self):
        return round(sum(self.coral) / len(self.coral), 2)

    def get_avg_climbS(self):
        return round(sum(self.climbS) / len(self.climbS), 2)

    def get_avg_climbD(self):
        return round(sum(self.climbS) / len(self.climbS), 2)

    def get_avg_score(self):
        return round(sum(self.score) / len(self.score))


class MatchStruct:
    def __init__(self):
        self.teams = {}

    def add_team_cycles(self, team, coral, algae, total, climbS, climbD):
        t = self.teams.setdefault(team, {})
        t["coral"] = coral
        t["algae"] = algae
        t["total"] = total
        t["climbS"] = climbS
        t["climbD"] = climbD


class Processor:
    CORAL_COLUMNS = [
        "Coral AL1",
        "Coral AL2",
        "Coral AL3",
        "Coral AL4",
        "Coral TL1",
        "Coral TL2",
        "Coral TL3",
        "Coral TL4",
    ]
    ALGAE_COLUMNS = ["Algae P", "Algae B"]
    CORAL_VALS = [3, 4, 6, 7, 2, 3, 4, 5]
    ALGAE_VALS = [2, 4]

    def __init__(self, outpath, chunk_size, teams, sched):
        self.chunk_size = chunk_size
        self.outpath = outpath
        self._teamsAt = teams
        self._sched = sched
        self._teams = {}
        self._matches = {}

    def output_teams(self, outfile):
        with open(outfile, "w") as f:
            data = {
                "teams": [
                    {
                        k: {
                            "Average Algae": v.get_avg_alg(),
                            "Average Coral": v.get_avg_coral(),
                            "Average Cycles": v.get_avg_total(),
                            "Average Shallow": v.get_avg_climbS(),
                            "Average Deep": v.get_avg_climbD(),
                            "Average Score": v.get_avg_score(),
                        }
                    }
                    for k, v in self._teams.items()
                ]
            }
            data["Percent Teams Scouted"] = round(
                len([x for x in self._teams.keys() if x in self._teamsAt])
                / len(self._teamsAt),
                2,
            )
            json.dump(
                data,
                f,
                indent=4,
            )

    def predict_matches(self, outfile):
        data = []
        for match in self._sched:
            red_score = 0
            blue_score = 0
            for key in match["r"]:
                if int(key.removeprefix("frc")) in self._teams:
                    red_score += self._teams[
                        int(key.removeprefix("frc"))
                    ].get_avg_score()
            for key in match["b"]:
                if int(key.removeprefix("frc")) in self._teams:
                    blue_score += self._teams[
                        int(key.removeprefix("frc"))
                    ].get_avg_score()
            data.append(
                {
                    match["k"]: {
                        "Red Score": red_score,
                        "Blue Score": blue_score,
                        "Red Teams": [x.removeprefix("frc") for x in match["r"]],
                        "Blue Teams": [x.removeprefix("frc") for x in match["b"]],
                    }
                }
            )
        with open(outfile, "w") as f:
            json.dump(data, f, indent=4)

    def output_matches(self, outfile):
        # with open(outfile, "w") as f:
        #     json.dump(
        #         [
        #             {k: {
        #                 t: {
        #                     "Total Coral": str(v.teams[t]["coral"]),
        #                     "Total Algae": str(v.teams[t]["algae"]),
        #                     "Total Cycles": str(v.teams[t]["total"]),
        #                     "Shallow Climb": str(v.teams[t]["climbS"]),
        #                     "Deep Climb": str(v.teams[t]["climbD"]),
        #                 }
        #                 for t in v.teams.keys()
        #             }}
        #             for k, v in self._matches.items()
        #         ],
        #         f,
        #         indent=4,
        #     )
        pd.DataFrame({
            "Match": [k for k, v in self._matches.items() for _ in range(len(v.teams))],
            "Team": [t for v in self._matches.values() for t in v.teams.keys()],
            "Total Coral": [int(t["coral"]) for v in self._matches.values() for t in v.teams.values()],
            "Total Algae": [int(t["algae"]) for v in self._matches.values() for t in v.teams.values()],
            "Total Cycles": [int(t["total"]) for v in self._matches.values() for t in v.teams.values()],
            "Shallow Climb": [int(t["climbS"]) for v in self._matches.values() for t in v.teams.values()],
            "Deep Climb": [int(t["climbD"]) for v in self._matches.values() for t in v.teams.values()]
        }).to_csv(outfile, index=False)

    def proccess_data(self, data_filepath: str, outname):
        with pd.read_csv(
            data_filepath, chunksize=self.chunk_size, iterator=True
        ) as reader:
            first = True
            for chunk in reader:
                chunk["Total Coral"] = chunk[Processor.CORAL_COLUMNS].sum(axis=1)
                chunk["Total Algae"] = chunk[Processor.ALGAE_COLUMNS].sum(axis=1)
                chunk["Total Cycles"] = chunk["Total Coral"] + chunk["Total Algae"]
                chunk["Total Score"] = (
                    (chunk[Processor.CORAL_COLUMNS] * Processor.CORAL_VALS).sum(axis=1)
                    + (chunk[Processor.ALGAE_COLUMNS] * Processor.ALGAE_VALS).sum(
                        axis=1
                    )
                    + chunk["Mobility"] * 3
                    + chunk["Barge S"] * 6
                    + chunk["Barge D"] * 12
                )
                for team in chunk["Team Number"]:
                    self._teams.setdefault(team, TeamStruct()).extend_data(
                        chunk.loc[chunk["Team Number"] == team, "Total Coral"],
                        chunk.loc[chunk["Team Number"] == team, "Total Algae"],
                        chunk.loc[chunk["Team Number"] == team, "Barge S"],
                        chunk.loc[chunk["Team Number"] == team, "Barge D"],
                        chunk.loc[chunk["Team Number"] == team, "Total Score"],
                    )
                for match in chunk["Match Number"]:
                    for team in chunk.loc[
                        chunk["Match Number"] == match, "Team Number"
                    ]:
                        row = chunk.loc[
                            (chunk["Team Number"] == team)
                            & (chunk["Match Number"] == match)
                        ]
                        self._matches.setdefault(match, MatchStruct()).add_team_cycles(
                            team,
                            row.at[row.index[0], "Total Coral"],
                            row.at[row.index[0], "Total Algae"],
                            row.at[row.index[0], "Total Cycles"],
                            row.at[row.index[0], "Barge S"],
                            row.at[row.index[0], "Barge D"],
                        )
                chunk.to_csv(
                    os.path.join(self.outpath, outname),
                    mode="a",
                    index=False,
                    header=first,
                )
                first = False
        self.output_teams(os.path.join(self.outpath, outname + "-teams.json"))
        self.output_matches(os.path.join(self.outpath, outname + "-matches.csv"))
        self.predict_matches(os.path.join(self.outpath, outname + "-predict.json"))
