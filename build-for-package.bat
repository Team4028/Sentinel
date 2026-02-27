if not exist "dockerout" mkdir "dockerout"
docker buildx build -f .\dockerimage\dockerfile -t scouting:latest -o type=docker,dest=dockerout\scouting.tar .
docker buildx build -f .\grafana-dashboard\dockerfile -t grafana-scouting:latest -o type=docker,dest=dockerout\grafana-scouting.tar .