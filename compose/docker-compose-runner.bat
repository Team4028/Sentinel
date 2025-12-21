@echo off
REM hmm yes
setlocal enabledelayedexpansion
for %%f in (*.tar) do (
    docker load -i "%%f"
)

docker compose --profile prod up