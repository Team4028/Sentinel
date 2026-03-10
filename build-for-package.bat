mkdir dockerout\grafana-dashboard
docker buildx build -f .\dockerimage\dockerfile -t scouting:latest -o type=docker,dest=dockerout\scouting.tar .
docker buildx build -f .\grafana-dashboard\dockerfile -t grafana-scouting:latest -o type=docker,dest=dockerout\grafana-scouting.tar .
cp .\compose.yaml dockerout
cp .\compose\.env dockerout\.env
cp .\compose\run.sh dockerout\run.sh
cp .\compose\mount.bat dockerout\mount.bat
wsl chmod +x dockerout/run.sh
cp .\compose\run.bat dockerout\run.bat
cp .\grafana-dashboard\grafana.ini dockerout\grafana-dashboard\grafana.ini
7z a .\build\scouting-app.zip ".\dockerout\*"
rmdir /s /q dockerout