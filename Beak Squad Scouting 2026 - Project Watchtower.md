# Requirements

## INPUT Layer - iPads

### Must Have

* QRScout (or similar) app - local/offline data input that outputs csv data as QR code
* Must run on iPads for scouts to input data during matches

### Nice To Have

* Track auton lineup and/or scoring locations for coordination
* Direct upload button to be enabled for pre-scouting activities
* Import match schedule from TBA so match number and bot assignment populates team number
* Ability to run scouter app on a phone or laptop in a pinch if needed

## COLLECTION Layer - Stands Laptop (Laptop 1)

### Must Have

#### Hardware

* Team-owned laptop

  * Ideally 13" class, recent CPU and 8+ GB RAM. No GPU requirement
  * Windows 11 if possible, otherwise Linux - Mint, Ubuntu, Debian, or similar

* Uses USB QR code scanner to input match data from scout iPads into a spreadsheet
* Spreadsheet must be CSV format and raw match data only

#### Data transfer to Laptop 2

* Primary: Meshtastic (or similar) wireless transmission using LoRA messaging
* Backup: Export data to USB stick or SD Card and send runner to pit after every 1-2 matches

### Nice To Have

* Automatic checking of inputs, ensuring all 6 bots scouted each match and no duplicates
* Automatic data validation to ensure "realistic" data has been provided
* Option to connect to internet to upload data, for use when cell service at the venue is good

  * Would require the top 2 layers to be hosted online as well as on laptop 2

* Copy of top 2 layers software to run all on 1 laptop in case of emergency



## TRANSFORMATION Layer - Pit Laptop (Laptop 2)

### Must Have

#### Hardware

* Team-owned laptop

  * Ideally 13" class, recent CPU and 8+ GB RAM. No GPU requirement
  * Windows 11 if possible, otherwise Linux - Mint, Ubuntu, Debian, or similar

#### Software

##### Front-End

* Upload screen to upload/import data
* Admin view to audit/modify input and/or output data as-needed

##### Back-End

* Transforms the raw match data into structured data that includes derived fields such as averages, rankings, percentiles, trends, etc.
* Flask app to coordinate frontend and backend and data transfer to Grafana
* pandas for data transformation
* Derived fields must be clearly identified/documented and easily maintainable - what fields we want and how we calculate with/against them will change every season
* Ability to export the input and output data as-needed for auditing or backup/migration purposes

###### Data Retrieval From Laptop 1

* Primary: Wireless data retrieval via Meshtastic (or similar)
* Backup: import data from USB stick or SD Card via frontend interface
* Internet: For venues with good cell service, data upload happens via internet

### Nice To Have

* password/login protection for upload screen AND admin views
* Potentially leverage machine learning libraries/tools for more interesting data insights
* Maintain an ongoing database of team/match data to easily cross-compare across multiple events and make pre-scouting easier

  * Could also just have all data in the same "table" with a field for the event code to filter against

* When connected to the internet, fetch data from TBA for additional insights and tracking current match
* Copy of software running in the cloud, so we can pivot to online vs offline systems as needed based on venue.

## DISPLAY Layer - Pit Laptop (Laptop 2)

### Must Have

* Grafana for data visualization
* Intakes data from the transformation layer and displays a number of various charts and views on multiple dashboards
* Pre-Match Strat dashboard for viewing the relevant data for our next upcoming match
* Friday-Night Strategy Discussion dashboard for assessing the field
* Dashboard with raw data view
* Admin control for changing charts to prevent users/viewers from changing layouts

### Nice To Have

* Picklist Creation Tool/Dashboard

  * Ability to update/cross-out teams during Alliance Selection

* Copy of software running in the cloud for team-wide access during Friday-night strategy sessions

  * Concurrent viewing for multiple users without causing view/filter conflicts

* Admin control for changing charts to prevent users/viewers from changing layouts

# Whiteboard

![Obelisk.jpg](https://github.com/Team4028/Sentinel/blob/main/Obelisk.jpg)
