@echo off
setlocal

set "SCRIPT_DIR=%~dp0"
for %%I in ("%SCRIPT_DIR%..") do set "PROJECT_ROOT=%%~fI"
set "KAFKA_DIR=%PROJECT_ROOT%\kafka"

set "BOOTSTRAP_SERVER=%~1"
if "%BOOTSTRAP_SERVER%"=="" set "BOOTSTRAP_SERVER=localhost:9092"

set "TOPIC_NAME=%~2"
if "%TOPIC_NAME%"=="" set "TOPIC_NAME=stock-trades"

:: Set JAVA_HOME if not defined
if "%JAVA_HOME%"=="" (
    for /f "tokens=*" %%g in ('where java 2^>nul') do (
        for %%h in ("%%~dpg..") do (
            if not defined JAVA_HOME set "JAVA_HOME=%%~fh"
        )
    )
)

echo Describing topic '%TOPIC_NAME%' on %BOOTSTRAP_SERVER%...

call "%KAFKA_DIR%\bin\windows\kafka-topics.bat" --bootstrap-server %BOOTSTRAP_SERVER% --describe --topic %TOPIC_NAME%
