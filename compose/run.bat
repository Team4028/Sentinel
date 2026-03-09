@echo off
REM hmm yes
setlocal enabledelayedexpansion

docker images | findstr /C:"scouting" >nul 2>&1
if %errorlevel%==0 (
    set /p "skip=Docker images detected, skip installation? (y/n): "
    if "!skip!" == "y" goto skipped
    if "!skip!" == "Y" goto skipped
)

for %%f in (*.tar) do (
    docker load -i "%%f"
)

:skipped
wsl -l >nul 2>&1
if %errorlevel%==0 (
    call .\mount.bat
) else (
    echo "Error: No WSL2 distro detected: please install to pass USB devices to Docker"
    exit 1
)

docker compose --profile prod up