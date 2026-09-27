@echo off
setlocal enabledelayedexpansion

set "SCRIPT_DIR=%~dp0"
for %%I in ("%SCRIPT_DIR%..") do set "PROJECT_ROOT=%%~fI"
set "KAFKA_DIR=%PROJECT_ROOT%\kafka"
set "KAFKA_VERSION=3.8.0"
set "SCALA_VERSION=2.13"
set "KAFKA_ARCHIVE=kafka_%SCALA_VERSION%-%KAFKA_VERSION%.tgz"
set "DOWNLOAD_URL=https://archive.apache.org/dist/kafka/%KAFKA_VERSION%/%KAFKA_ARCHIVE%"
set "FORMAT_MARKER=%KAFKA_DIR%\.kraft_formatted"

echo ============================================================
echo  Setting up Apache Kafka (KRaft mode) natively on Windows
echo ============================================================

:: Verify Java installation
where java >nul 2>&1
if %errorlevel% neq 0 (
    echo ERROR: java command not found in PATH. Please install Java 11 or 17.
    exit /b 1
)

:: Automatically discover and export JAVA_HOME if not defined
if "%JAVA_HOME%"=="" (
    for /f "tokens=*" %%g in ('where java 2^>nul') do (
        for %%h in ("%%~dpg..") do (
            if not defined JAVA_HOME set "JAVA_HOME=%%~fh"
        )
    )
)
echo Using Java from: %JAVA_HOME%

:: Download Kafka archive if not present
if not exist "%KAFKA_DIR%\bin\windows\kafka-storage.bat" (
    echo Kafka not found in %KAFKA_DIR%. Downloading Kafka %KAFKA_VERSION%...
    if not exist "%PROJECT_ROOT%\tmp_kafka" mkdir "%PROJECT_ROOT%\tmp_kafka"
    cd /d "%PROJECT_ROOT%\tmp_kafka"

    curl.exe -fSL -o "%KAFKA_ARCHIVE%" "%DOWNLOAD_URL%"
    if %errorlevel% neq 0 (
        echo ERROR: Failed to download %DOWNLOAD_URL%
        cd /d "%PROJECT_ROOT%"
        exit /b 1
    )

    echo Extracting %KAFKA_ARCHIVE%...
    tar.exe -xzf "%KAFKA_ARCHIVE%"
    if not exist "%KAFKA_DIR%" mkdir "%KAFKA_DIR%"
    xcopy /E /I /Y "kafka_%SCALA_VERSION%-%KAFKA_VERSION%\*" "%KAFKA_DIR%\" >nul
    cd /d "%PROJECT_ROOT%"
    echo Applying Windows classpath and WMIC fixes to Kafka batch files...
    powershell -NoProfile -Command "$path = Join-Path $env:KAFKA_DIR 'bin\windows\kafka-run-class.bat'; $c = Get-Content $path -Raw; $c = $c -replace 'for %%%%i in \(\"%%BASE_DIR%%\\libs\\\*\"\) do \(\s*call :concat \"%%%%i\"\s*\)', 'set CLASSPATH=%%BASE_DIR%%\libs\*'; $c = $c -replace 'set KAFKA_LOG4J_OPTS=-Dlog4j.configuration=file:%%BASE_DIR%%/config/tools-log4j.properties', 'set KAFKA_LOG4J_OPTS=\"-Dlog4j.configuration=file:%%BASE_DIR%%/config/tools-log4j.properties\"'; $c = $c -replace 'set KAFKA_LOG4J_OPTS=-Dkafka.logs.dir=\"%%LOG_DIR%%\" \"%%KAFKA_LOG4J_OPTS%%\"', 'set KAFKA_LOG4J_OPTS=\"-Dkafka.logs.dir=%%LOG_DIR%%\" %%KAFKA_LOG4J_OPTS%%'; Set-Content -Path $path -Value $c"
    powershell -NoProfile -Command "$path = Join-Path $env:KAFKA_DIR 'bin\windows\kafka-server-start.bat'; $c = Get-Content $path -Raw; $c = $c -replace 'set KAFKA_LOG4J_OPTS=-Dlog4j.configuration=file:%%~dp0../../config/log4j.properties', 'set KAFKA_LOG4J_OPTS=\"-Dlog4j.configuration=file:%%~dp0../../config/log4j.properties\"'; $c = $c -replace '(?s)IF \[\"%%KAFKA_HEAP_OPTS%%\"\] EQU \[\"\"\] \(.*?wmic os get osarchitecture.*?\)', 'IF [\"%KAFKA_HEAP_OPTS%\"] EQU [\"\"] (`n    set KAFKA_HEAP_OPTS=-Xmx1G -Xms1G`n)'; Set-Content -Path $path -Value $c"
    powershell -NoProfile -Command "$path = Join-Path $env:KAFKA_DIR 'bin\windows\kafka-server-stop.bat'; $c = Get-Content $path -Raw; $c = $c -replace 'wmic process where \(commandline like \"%%%%kafka.Kafka%%%%\" and not name=\"wmic.exe\"\) delete', 'powershell -NoProfile -Command \"Get-CimInstance Win32_Process | Where-Object { `$_.CommandLine -like ''*kafka.Kafka*'' } | ForEach-Object { Stop-Process -Id `$_.ProcessId -Force }\"'; Set-Content -Path $path -Value $c"
    echo Kafka successfully installed and configured in %KAFKA_DIR%
) else (
    echo Kafka already present in %KAFKA_DIR%.
)

:: Format storage for KRaft mode
if exist "%FORMAT_MARKER%" (
    echo KRaft storage already formatted. Skipping format step.
) else (
    echo Formatting KRaft storage directories...
    for /f "delims=" %%I in ('call "%KAFKA_DIR%\bin\windows\kafka-storage.bat" random-uuid') do (
        set "CLUSTER_ID=%%I"
    )
    echo Generated KRaft Cluster ID: !CLUSTER_ID!
    call "%KAFKA_DIR%\bin\windows\kafka-storage.bat" format -t !CLUSTER_ID! -c "%KAFKA_DIR%\config\kraft\server.properties"
    if %errorlevel% neq 0 (
        echo ERROR: Failed to format KRaft storage.
        exit /b 1
    )
    type nul > "%FORMAT_MARKER%"
    echo KRaft storage formatted successfully.
)

echo ============================================================
echo  Setup complete!
echo  To start the Kafka broker, run:
echo    scripts\start_kafka.bat
echo ============================================================
