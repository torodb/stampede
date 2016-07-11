#!/bin/bash

cp -r $(ls -d /toro/torodb-*-SNAPSHOT)/* /toro_dist

POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-}"
echo "postgres:5432:*:postgres:$POSTGRES_PASSWORD" > ~/.pgpass
chmod 400 ~/.pgpass
echo -n "Waiting PostgreSQL..."
while ! psql -h postgres -U postgres -c "SELECT 1" > /dev/null 2>&1
do
	sleep 1
	echo -n .
done
echo

TORODB_PASSWORD="${TORODB_PASSWORD:-trustme}"
echo "postgres:5432:torod:torodb:$TORODB_PASSWORD" > ~/.toropass
chown 400 ~/.toropass
psql -h postgres -U postgres -c "CREATE USER torodb PASSWORD '$TORODB_PASSWORD'"
psql -h postgres -U postgres -c "CREATE DATABASE torod OWNER torodb"

rm ~/.pgpass
POSTGRES_PASSWORD=
TORODB_PASSWORD=
