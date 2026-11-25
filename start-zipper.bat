@echo off
rem Zipper sidecar launcher — installs deps on first run, then starts the poller.
rem Requires Node >= 20 and Streamer.bot running with its WebSocket Server on :8080.
setlocal
cd /d "%~dp0"
rem Stop before npm or the sidecar runs when Node is missing or older than 20.
set "ZIPPER_NODE_VERSION="
for /f "delims=" %%v in ('node --version 2^>nul') do if not defined ZIPPER_NODE_VERSION set "ZIPPER_NODE_VERSION=%%v"
set "ZIPPER_NODE_MAJOR=0"
if defined ZIPPER_NODE_VERSION for /f "tokens=1 delims=v." %%m in ("%ZIPPER_NODE_VERSION%") do set /a "ZIPPER_NODE_MAJOR=%%m" 2>nul
if %ZIPPER_NODE_MAJOR% LSS 20 goto need_node
if not exist node_modules (
  echo Installing dependencies...
  call npm install --no-audit --no-fund
)
node zipper-sidecar.mjs
pause
exit /b

:need_node
if not defined ZIPPER_NODE_VERSION set "ZIPPER_NODE_VERSION=none"
echo Zipper needs Node.js 20 or later (found: %ZIPPER_NODE_VERSION%). Get it from https://nodejs.org and run this again.
pause
exit /b 1
