import os

import numpy as np
import yaml
from enum import Enum
import pandas as pd
import traceback
import re
import logging

logger = logging.getLogger(__name__)


def read_config(year: str):
    """Reads the configuration YAML file into memory"""
    try:
        with open(os.path.join("config", f"field-config-{year}.yaml"), 'r') as f:
            data = yaml.safe_load(f)
        return data
    except FileNotFoundError:
        logger.error("Error: File not found at path")
    except yaml.YAMLError as e:
        logger.error(f"YAML Parser encountered an error: {e}")


FILTERS = ["avg", "max", "fil"]

SVD_AUGS = ["variance-score", "stability"]

# more human readable names for the yaml filters
FANCY_FIL = {"avg": "Average", "max": "Max", "fil": "Filtered"}

def get_svd_headers(svd) -> list[str]:
        """ adds a 'x Variance' header to the input svd header to account for the variance metrics generated """
        x = [svd["name"]]
        if "variance-score" in svd:
            x.append(svd["name"] + " Variance")
        return x

class GrafanaDataPreset(Enum):
    """ Defines a list of grafana field presets to auto-cast different datasets into numbers or other types """

    MATCH = lambda data: zip(
        [x["name"] for x in data["match-fields"]],
        ["number" for _ in data["match-fields"]],
    )
    TEAM = lambda data: ([
        ("Rank", "number"),
        ("Average RP", "number"),
        ("OPR", "number"),
        ("Last OPR", "number"),
    ]
    + (list(zip(
        l := [s for s in data["copr-keys"]],
        ["number" for _ in l]
    ))) if "copr-keys" in data else list()
    + (list(zip(
        l := [_ for svd in data["subjective-svd-fields"] for _ in get_svd_headers(svd)], # walrus to avoid reparse list
        ["number" for _ in l]
    )) if "subjective-svd-fields" in data else list())
    + list(
        zip(
            l := [
                FANCY_FIL[f] + " " + x["name"]
                for f in FILTERS
                for x in data["team-fields"]
                if f in x
            ],
            ["number" for _ in l],
        )
    ))  # fill in the blanks type for loop

    PREMATCH = lambda data: ([
        ("OPR", "number"),
    ]
    + (list(zip(
        l := [s for s in data["copr-keys"]],
        ["number" for _ in l]
    ))) if "copr-keys" in data else list()
    + list(zip(
        [x["name"] for x in data["depth-predict-fields"]],
        ["number" for _ in data["depth-predict-fields"]],
    )))

    PREMATCH_SCORE = lambda _: [
        ("1 Score", "number"),
        ("2 Score", "number"),
        ("3 Score", "number"),
        ("Won", "boolean"),
    ]

    NONE = lambda _: []


def lex_config(year: str):
    """reads and restructures the YAML for use by the Processor"""
    data = read_config(year)
    config = {
        "compute": [],
        "headers": [],
        "svd": [],
        "teams": [],
        "matches": [],
        "predict-metric": "",
        "uniques": [],
        "preproc": [],
        "dash-panel": {},
        "deep-predict": [],
        "copr": [],
        "tests": [],
        "pre-tests": [],
    }
    if data:
        for val in filter(lambda x: not x[0].startswith("_"), GrafanaDataPreset.__dict__.items()):
            config["dash-panel"][val[0]] = val[1](data)
        config["tn"] = data["team-header-name"]
        config["mn"] = data["match-header-name"]
        config["si"] = data["si-header-name"]
        for field in data["headers"]:
            config["headers"].append(field["name"])
        if "preproc-operations" in data:
            for field in data["preproc-operations"]:
                _data = {"name": field["name"], "op": field["operation"]}
                if "new-headers" in field:
                    _data |= {"new-headers": field["new-headers"]}
                config["preproc"].append(_data)
        for field in data["compute-fields"]:
            config["compute"].append({"name": field["name"], "eq": field["equation"]})
        config["uniques"] = data["filter-unique-fields"]
        if "unique-fields-post-svd" in data:
            config["uniques-post"] = data["unique-fields-post-svd"]
        if 'copr-keys' in data:
            config['copr'] = data['copr-keys']
        if "subjective-svd-fields" in data:
            for field in data["subjective-svd-fields"]:
                config["svd"].append(
                    {
                        "name": field["name"],
                        "source": field["source"],
                        "comp-team": field["compare-team-source"],
                        "augs": [x for x in SVD_AUGS if x in field],
                    }
                )
        for field in data["team-fields"]:
            config["teams"].append(
                {
                    "name": field["name"],
                    "filters": [x for x in FILTERS if x in field],
                    "derive": field["derive"],
                }
            )
        if "data-tests" in data:
            for test in data["data-tests"]:
                config["tests"].append(
                    {
                        "name": test["name"],
                        "expr": test["expression"],
                    }
                )
        if "prelim-tests" in data:
            for test in data["prelim-tests"]:
                config["pre-tests"].append(
                    {
                        "name": test["name"],
                        "expr": test["expression"],
                    }
                )
        for field in data["match-fields"]:
            config["matches"].append(
                {
                    "name": field["name"],
                    "derive": field["derive"],
                    "filters": [x for x in FILTERS if x in field],
                }
            )
        config["p-metric"] = data["predict-metric"]
        for field in data["depth-predict-fields"]:
            config["deep-predict"].append(
                {"name": field["name"], "source": field["source"]}
            )
    logger.info(f"Successfully loaded config for year: {year}")
    return config