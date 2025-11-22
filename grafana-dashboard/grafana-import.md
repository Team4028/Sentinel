# Importing Dashboard Configs into Grafana

## Dashboard Layout

- Click the + in the top right of the screen
- Select Import Dashboard
- Paste in or upload the JSON file

## $ Data\text{ }Sources $
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