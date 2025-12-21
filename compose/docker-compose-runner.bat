@echo off
REM hmm yes
setlocal enabledelayedexpansion
for %%f in (*.tar) do (
    docker load -i "%%f"
)

if not exist ".\secrets" (
    mkdir ".\secrets"
)

if not exist ".\secrets\key.txt" (
    set /p API_KEY=Enter TBA key: 
    set "API_KEY=!API_KEY: =!"
    echo !API_KEY! > ".\secrets\key.txt"
)

docker compose --profile prod up