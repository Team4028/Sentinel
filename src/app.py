from flask import Flask, request, render_template, jsonify, Response
from src.lib.data_main import Processor
import os
import requests
import json
import re

def create_app(): # cursed but whatever

    app = Flask(__name__)
    UPL_DIR = "datain"
    OUT_DIR = "dataout"
    CHUNK = 10000
    EVENT_KEY = "2025iri"

    if (os.path.exists("./key.txt")):
        with open("./key.txt", 'r') as f:
            auth_key = f.read().strip()
    else: auth_key = input("Enter TBA Auth key: ")

    SERIES = ["Teams", "Matches", "Predictions"]

    teams = [
        x["team_number"]
        for x in requests.get(
            f"https://www.thebluealliance.com/api/v3/event/{EVENT_KEY}/teams",
            headers={"X-TBA-Auth-Key": auth_key},
        ).json()
    ]

    def sched_sorter(match):
        key = match["k"].removeprefix(EVENT_KEY + "_")
        order = {"qm": 0, "sf": 1, "f": 2}

        if key.startswith("qm"):
            x = int(key[2:])
            return (order["qm"], x, 0)
        else:
            m = re.match(r"(sf|f)(\d+)m(\d+)", key)
            if m:
                prefix, round, idx = m.groups()
                return (order[prefix], int(round), int(idx))
            else:
                return (99, 0, 0)

    schedule = sorted([
        {
            "k": x["key"],
            "r": x["alliances"]["red"]["team_keys"],
            "b": x["alliances"]["blue"]["team_keys"],
        }
        for x in requests.get(
            f"https://www.thebluealliance.com/api/v3/event/{EVENT_KEY}/matches",
            headers={"X-TBA-Auth-Key": auth_key},
        ).json()
    ], key=sched_sorter)

    processor = Processor(OUT_DIR, CHUNK, teams, schedule)
    infile = os.path.join(UPL_DIR, "data_in.csv")
    js = None

    def stream(file):
        with open(file, 'rb') as r:
            while chunk := r.read(8192):
                yield chunk

    def reload_js():
        global js
        if not os.path.exists(os.path.join(OUT_DIR, "other-metrics.json")):
            js = None
            return
        with open(os.path.join(OUT_DIR, "other-metrics.json"), "r") as r:
            js = json.load(r) if os.path.exists(os.path.join(OUT_DIR, "other-metrics.json")) else None

    reload_js()


    def de_prettify_series(series):
        match (series):
            case "Teams":
                return "teams"
            case "Matches":
                return "matches"
            case "Predictions":
                return "predict"
        return ""


    @app.route("/")
    def main():
        return render_template("home.html")

    @app.get("/percent")
    def percent():
        return jsonify(js) if js else ""

    @app.post("/upload")
    def upload_file():
        if "data" in request.files:
            d_file = request.files["data"]
            if d_file.filename != "":
                d_file.save(infile)
                processor.proccess_data(infile, "output.csv")
                reload_js()
        return "", 200

    @app.route("/reproc")
    def reprocess():
        processor.proccess_data(infile, "output.csv")
        reload_js()
        return "Data reloaded."

    @app.route("/team-meta")
    def tmeta():
        if (os.path.exists(os.path.join(OUT_DIR, "output.csv-teams.csv"))):
            with open(os.path.join(OUT_DIR, "output.csv-teams.csv")) as r:
                headers = r.readline().strip().split(",")
                headers.remove("Team")
                return jsonify(headers)
        return ""

    @app.route("/match-meta")
    def mmeta():
        if (os.path.exists(os.path.join(OUT_DIR, "output.csv-matches.csv"))):
            with open(os.path.join(OUT_DIR, "output.csv-matches.csv")) as r:
                headers = r.readline().strip().split(",")
                headers.remove("Match")
                headers.remove("Team")
                return jsonify(headers)
        return ""

    @app.get("/next-3")
    def n3():
        if not request.args["mkey"]: return ""
        team = request.args["team"] or -1
        curr_match = request.args["mkey"]
        foundit = False
        next_3 = []
        for m in schedule:
            if foundit:
                if len(next_3) >= 3:
                    break
                if team == -1 or (("frc" + team) in m['b']) or (("frc" + team) in m['r']):
                    next_3.append(m['k'])
            elif m['k'] == curr_match:
                foundit = True
        return jsonify(next_3)

    @app.route("/append", methods=["POST"])
    def append_line():
        if "data" in request.files:
            d_file = request.files["data"]
            exists = os.path.exists(infile)
            with open(infile, 'a' if exists else 'w') as append:
                lines_to_write = [l.decode("utf-8") for l in d_file.readlines() if l and ((not exists) or (not "MN" in l.decode("utf-8")))]
                if len(lines_to_write) > 0:
                    if exists: append.write("\n")
                    append.writelines(lines_to_write) # dodge header if exists
            processor.proccess_data(infile, "output.csv")
            reload_js()
        return "", 200

    @app.get("/download/<file>")
    def dload(file):
        if not file: return Response("File not found.", 403)
        file = os.path.basename(file) # no .. touchy
        file = os.path.join(OUT_DIR if "output" in file else UPL_DIR, file)
        if not os.path.exists(file): return Response("File not found.", 403)
        return Response(stream(file), mimetype=('text/json' if ".json" in file else 'text/csv'), headers={
            'Content-Disposition': f"attachment; filename={os.path.basename(file)}"
        })
    return app


if __name__ == "__main__":
    create_app().run(port=5001, use_reloader=False)
