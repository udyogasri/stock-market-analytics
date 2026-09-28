@echo off
setlocal

set "SCRIPT_DIR=%~dp0"
for %%I in ("%SCRIPT_DIR%..") do set "PROJECT_ROOT=%%~fI"
set "KAFKA_DIR=%PROJECT_ROOT%\kafka"

echo Stopping Kafka broker gracefully...
call "%KAFKA_DIR%\bin\windows\kafka-server-stop.bat"
echo Kafka stop signal sent.
