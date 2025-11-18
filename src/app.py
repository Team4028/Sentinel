from flask import Flask, request, render_template, jsonify
from lib.data_main import Processor
import os
import csv
import requests
import json

app = Flask(__name__)
app.config["UPL_DIR"] = "datain"
app.config["OUT_DIR"] = "dataout"
app.config["CHUNK"] = 10000
app.config["EVENT_KEY"] = "2025iri"

auth_key = input("Enter TBA Auth key: ")

teams = [x["team_number"] for x in requests.get(f"https://www.thebluealliance.com/api/v3/event/{app.config["EVENT_KEY"]}/teams", headers={
    "X-TBA-Auth-Key": auth_key
}).json()]
schedule = [{"k": x["key"], "r": x["alliances"]["red"]["team_keys"], "b": x["alliances"]["blue"]["team_keys"]} for x in requests.get(f"https://www.thebluealliance.com/api/v3/event/{app.config["EVENT_KEY"]}/matches", headers={
    "X-TBA-Auth-Key": auth_key
}).json()]
processor = Processor(app.config["OUT_DIR"], app.config["CHUNK"], teams, schedule)
infile = os.path.join(app.config["UPL_DIR"], "data_in.csv")
json_rests = ["", ""]



def reload_rests():
    with open(app.config["OUT_DIR"] + "/output.csv-teams.json", 'r') as f:
            json_rests[0] = json.load(f)
    with open(app.config["OUT_DIR"] + "/output.csv-predict.json", 'r') as f:
            json_rests[1] = json.load(f)

if os.path.exists(app.config["OUT_DIR"] + "/output.csv-teams.json"):
     reload_rests()

@app.route("/")
def main():
    return render_template("home.html")

@app.route("/upload", methods=["POST"])
def upload_file():
    if "data" in request.files:
        d_file = request.files["data"]
        if d_file.filename != "":
            d_file.save(infile)
            processor.proccess_data(infile, "output.csv")
            reload_rests()
    return "", 200

@app.route("/outputs/<file>")
def show_file(file):
    match(file):
         case "teams":
              if (json_rests[0]):
                return jsonify(json_rests[0])
              return "Error. No data here."
         case "predict":
              if (json_rests[1]):
                return jsonify(json_rests[1])
              return "Error. No data here."
    return "Error. No Data Here."

@app.route("/reproc")
def reprocess():
    processor.proccess_data(infile, "output.csv")
    reload_rests()
    return "Request Recieved!"

@app.route("/append", methods=["POST"])
def append_line():
    if (request.args["line"]):
        with open(infile, "a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(request.args["line"].split(","))
    processor.proccess_data(infile, "output.csv")
    reload_rests()
    return "", 200
    

if __name__ == "__main__":
    app.run(port = 5001, use_reloader=False)