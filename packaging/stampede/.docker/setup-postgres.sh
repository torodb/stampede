#!/bin/bash

set -e

echo -n "Waiting PostgreSQL..."
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-}"
echo "postgres:5432:*:postgres:$POSTGRES_PASSWORD" > ~/.pgpass
chmod 400 ~/.pgpass
max_seconds=15
while ! /usr/bin/psql -h postgres -U postgres -c "SELECT 1" > /dev/null 2>&1
do
    sleep 1
    echo -n .
    max_seconds=$((max_seconds-1))
    if [ "$max_seconds" -le 0 ]
    then
        echo timeout
        exit 1
    fi
done
echo ok

echo -n "Setting up PostgreSQL..."
TORODB_PASSWORD="${TORODB_PASSWORD:-trustme}"
echo "postgres:5432:torod:torodb:$TORODB_PASSWORD" > ~/.toropass
chown 400 ~/.toropass
/usr/bin/psql -h postgres -U postgres -c "CREATE USER torodb PASSWORD '$TORODB_PASSWORD'"
/usr/bin/psql -h postgres -U postgres -c "CREATE DATABASE torod OWNER torodb"
echo ok

rm ~/.pgpass
POSTGRES_PASSWORD=
TORODB_PASSWORD=
