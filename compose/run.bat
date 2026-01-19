@echo off
REM hmm yes
setlocal enabledelayedexpansion
for %%f in (*.tar) do (
    docker load -i "%%f"
)


wsl -l >nul 2>&1
if %errorlevel%==0 (
    .\mount.bat
) else (
    echo "Error: No WSL2 distro detected: please install to pass USB devices to Docker"
    exit 1
)

docker compose --profile prod up