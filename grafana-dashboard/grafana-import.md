# Importing Dashboard Configs into Grafana

## Dashboard Layout

- Click the + in the top right of the screen
- Select Import Dashboard
- Paste in or upload the JSON files in [grafana-dashboard](https://github.com/Team4028/Sentinel/tree/main/grafana-dashboard)

## Ini
Copy the [grafana-dashboard/grafana.ini](https://github.com/Team4028/Sentinel/blob/main/grafana-dashboard/grafana.ini) file into the file at the below location, depending on OS
- windows: \<GRAFANA_INSTALL_LOCATION>\conf\defaults.ini (ex. C:\Program Files\GrafanaLabs\grafana\conf\defaults.ini)
- linux: /etc/grafana/grafana.ini or /usr/local/etc/grafana/grafana.ini
- mac: /usr/local/etc/grafana/grafana.ini or /opt/homebrew/etc/grafana/grafana/ini

## $Data\text{ }Sources$
While it's definitely easier to just make them from scratch, data sources can be imported by curling `/api/datasources` with the datasource JSON and bearer token:
### Example:
```http
POST /api/datasources HTTP/1.1
Accept: application/json
Content-Type: application/json
Authorization: Bearer eyJrIjoiT0tTcG1pUlY2RnVKZTFVaDFsNFZXdE9ZWmNrMkZYbk

{
  "name":"test_datasource",
  "type":"graphite",
  "url":"http://mydatasource.com",
  "access":"proxy",
  "basicAuth":false
}
```

Additionally, this data is accessible by `GET` ing `/api/datasources` or visiting it in your browser.

# EDIT
## With the grafana docker image distributed, this is not necessary