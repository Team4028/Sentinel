@echo off
REM Attach Meshtastic radio to WSL2 via usbipd

usbipd list >nul 2>&1

if %errorlevel% NEQ 0 (
    winget install --id dorssel.usbipd-win
)

for /f "tokens=1" %%a in ('usbipd list ^| findstr /C:"USB to UART Bridge"') do set USBID=%%a
sudo -V >nul 2>&1
if %errorlevel% NEQ 0 (
    winget install gerardog.gsudo
)
sudo usbipd bind -b %USBID%
echo Please continue after the suspicious looking admin cmd window closes
pause
usbipd attach -w -b %USBID%