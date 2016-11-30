@REM ----------------------------------------------------------------------------
@REM  Copyright 2001-2006 The Apache Software Foundation.
@REM
@REM  Licensed under the Apache License, Version 2.0 (the "License");
@REM  you may not use this file except in compliance with the License.
@REM  You may obtain a copy of the License at
@REM
@REM       http://www.apache.org/licenses/LICENSE-2.0
@REM
@REM  Unless required by applicable law or agreed to in writing, software
@REM  distributed under the License is distributed on an "AS IS" BASIS,
@REM  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
@REM  See the License for the specific language governing permissions and
@REM  limitations under the License.
@REM ----------------------------------------------------------------------------
@REM
@REM   Copyright (c) 2001-2006 The Apache Software Foundation.  All rights
@REM   reserved.

@echo off

if "%DEBUG%"=="true" @echo on

set ERROR_CODE=0

:init
@REM Decide how to startup depending on the version of windows

@REM -- Win98ME
if NOT "%OS%"=="Windows_NT" goto Win9xArg

@REM set local scope for the variables with windows NT shell
if "%OS%"=="Windows_NT" @setlocal

@REM -- 4NT shell
if "%eval[2+2]" == "4" goto 4NTArgs

@REM -- Regular WinNT shell
set CMD_LINE_ARGS=%*
goto WinNTGetScriptDir

@REM The 4NT Shell from jp software
:4NTArgs
set CMD_LINE_ARGS=%$
goto WinNTGetScriptDir

:Win9xArg
@REM Slurp the command line arguments.  This loop allows for an unlimited number
@REM of arguments (up to the command line limit, anyway).
set CMD_LINE_ARGS=
:Win9xApp
if %1a==a goto Win9xGetScriptDir
set CMD_LINE_ARGS=%CMD_LINE_ARGS% %1
shift
goto Win9xApp

:Win9xGetScriptDir
set SAVEDIR=%CD%
%0\
cd %0\.. 
set PRGDIR=%CD%
cd %0\..\.. 
set BASEDIR=%CD%
cd %SAVEDIR%
set SAVE_DIR=
goto setup

:WinNTGetScriptDir
set BASEDIR=%~dp0\..
set PRGDIR=%~dp0

:setup

cmd /c "%PRGDIR%\@{assembler.name}" --version > nul
if %ERRORLEVEL% NEQ 0 (
    echo %PRGDIR%\@{assembler.name} not working as expected
    echo Please check Java installation and that java.exe path is present in PATH environment variable 
    exit /b 1
)

if NOT "%POSTGRES_HOME%"=="" (
    set PSQL=%POSTGRES_HOME%\bin\psql.exe
) else (
    set PSQL=psql
)

"%PSQL%" --version > nul
if %ERRORLEVEL% NEQ 0 (
    echo %PSQL% not found
    echo Please check PostgreSQL installation and that installation path match POSTGRES_HOME environment variable or psql.exe path is present in PATH environment variable 
    exit /b 1
)

FOR /F "tokens=* USEBACKQ" %%F IN (`cmd /c "%PRGDIR%\@{assembler.name}" %CMD_LINE_ARGS% -lp /backend/postgres/port`) DO ( SET POSTGRES_PORT=%%F )
set "POSTGRES_PORT=%POSTGRES_PORT:~0,-1%"
FOR /F "tokens=* USEBACKQ" %%F IN (`cmd /c "%PRGDIR%\@{assembler.name}" %CMD_LINE_ARGS% -lp /backend/postgres/user`) DO ( SET POSTGRES_USER=%%F )
set "POSTGRES_USER=%POSTGRES_USER:~0,-1%"
FOR /F "tokens=* USEBACKQ" %%F IN (`cmd /c "%PRGDIR%\@{assembler.name}" %CMD_LINE_ARGS% -lp /backend/postgres/host`) DO ( SET POSTGRES_HOST=%%F )
set "POSTGRES_HOST=%POSTGRES_HOST:~0,-1%"
FOR /F "tokens=* USEBACKQ" %%F IN (`cmd /c "%PRGDIR%\@{assembler.name}" %CMD_LINE_ARGS% -lp /backend/postgres/database`) DO ( SET POSTGRES_DATABASE=%%F )
set "POSTGRES_DATABASE=%POSTGRES_DATABASE:~0,-1%"

if "%TOROUSER%"=="" set TOROUSER=%USERNAME%

if "%TOROPASSFILE%"=="" set TOROPASSFILE=%HOMEDRIVE%%HOMEPATH%\.toropass

setlocal ENABLEEXTENSIONS ENABLEDELAYEDEXPANSION
set alfanum=ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789

set torodb_password=
FOR /L %%b IN (0, 1, 10) DO (
SET /A rnd_num=!RANDOM! * 62 / 32768 + 1
for /F %%c in ('echo %%alfanum:~!rnd_num!^,1%%') do set torodb_password=!torodb_password!%%c
)

@REM Reaching here means variables are defined and arguments have been captured
:endInit

"%PSQL%" -U postgres -h %POSTGRES_HOST% -p %POSTGRES_PORT% --no-readline -c "SELECT 1" > nul
if %ERRORLEVEL% NEQ 0 goto PostgresNotRunnning
"%PSQL%" -U postgres -h %POSTGRES_HOST% -p %POSTGRES_PORT% --no-readline -d %POSTGRES_DATABASE% -c "SELECT 1" > nul
if %ERRORLEVEL% EQU 0 goto DatabaseAlreadyExists

echo Creating %POSTGRES_USER% user
"%PSQL%" -U postgres -h %POSTGRES_HOST% -p %POSTGRES_PORT% --no-readline -c "CREATE USER %POSTGRES_USER% WITH PASSWORD '%torodb_password%'"
echo Creating torod database
"%PSQL%" -U postgres -h %POSTGRES_HOST% -p %POSTGRES_PORT% --no-readline -c "CREATE DATABASE %POSTGRES_DATABASE% WITH OWNER %POSTGRES_USER%"

echo Creating %TOROPASSFILE% for user %TOROUSER%
echo %POSTGRES_HOST%:%POSTGRES_PORT%:%POSTGRES_DATABASE%:%POSTGRES_USER%:%torodb_password%> "%TOROPASSFILE%"
attrib +I +A "%TOROPASSFILE%"

set PGPASSWORD=%torodb_password%

"%PSQL%" -h %POSTGRES_HOST% -p %POSTGRES_PORT% --no-readline -U %POSTGRES_USER% -d %POSTGRES_DATABASE% -h %POSTGRES_HOST% -p 5432 -c "SELECT 1" > nul
if %ERRORLEVEL% NEQ 0 goto PostgresSecurityRestrictions
goto end

:error
if "%OS%"=="Windows_NT" @endlocal
set ERROR_CODE=%ERRORLEVEL%

:end
@REM set local scope for the variables with windows NT shell
if "%OS%"=="Windows_NT" goto endNT

@REM For old DOS remove the set variables from ENV - we assume they were not set
@REM before we started - at least we don't leave any baggage around
set CMD_LINE_ARGS=
goto postExec

:endNT
@REM If error code is set to 1 then the endlocal was done already in :error.
if %ERROR_CODE% EQU 0 @endlocal

:postExec

if "%FORCE_EXIT_ON_ERROR%" == "on" (
  if %ERROR_CODE% NEQ 0 exit %ERROR_CODE%
)

exit /b %ERROR_CODE%

:PostgresNotRunnning

echo PostgreSQL must be running to setup torodb
exit /b 1

:DatabaseAlreadyExists

echo Database %POSTGRES_DATABASE% already exists
exit /b 1

:PostgresSecurityRestrictions
echo Seem that we can not connect to PostgreSQL due to some security restrictions. Please add those lines to pg_hba.conf file:
echo
if "%POSTGRES_HOST%" == "localhost" ( 
    echo host    %POSTGRES_DATABASE%           %POSTGRES_USER%          ::1/128                 md5
    echo host    %POSTGRES_DATABASE%           %POSTGRES_USER%          127.0.0.1/32            md5
) else (
    echo host    %POSTGRES_DATABASE%           %POSTGRES_USER%          <PostgreSQL's machine IPv6>     md5
    echo host    %POSTGRES_DATABASE%           %POSTGRES_USER%          <PostgreSQL's machine IP>       md5
)
echo 
echo ...and then restart PostgreSQL server 
exit /b 1
