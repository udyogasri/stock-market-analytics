@echo off
setlocal

set "SCRIPT_DIR=%~dp0"
for %%I in ("%SCRIPT_DIR%..") do set "PROJECT_ROOT=%%~fI"
set "KAFKA_DIR=%PROJECT_ROOT%\kafka"

if not exist "%KAFKA_DIR%\config\kraft\server.properties" (
    echo ERROR: Kafka configuration not found. Please run scripts\setup_kafka.bat first.
    exit /b 1
)

:: Set JAVA_HOME if not defined
if "%JAVA_HOME%"=="" (
    for /f "tokens=*" %%g in ('where java 2^>nul') do (
        for %%h in ("%%~dpg..") do (
            if not defined JAVA_HOME set "JAVA_HOME=%%~fh"
        )
    )
)
if "%KAFKA_HEAP_OPTS%"=="" set "KAFKA_HEAP_OPTS=-Xmx1G -Xms1G"

echo ============================================================
echo  Starting Apache Kafka (KRaft mode) on localhost:9092
echo  Keep this terminal open for the entire session.
echo  Press Ctrl+C to stop the broker.
echo ============================================================

call "%KAFKA_DIR%\bin\windows\kafka-server-start.bat" "%KAFKA_DIR%\config\kraft\server.properties"
