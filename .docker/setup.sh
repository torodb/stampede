#!/bin/bash

cp -r $(ls -d /toro/torodb-*-SNAPSHOT)/* /toro_dist

echo postgres:5432:*:postgres:trustme > ~/.pgpass
chmod 400 ~/.pgpass
echo -n "Waiting PostgreSQL..."
while ! psql -h postgres -U postgres -c "SELECT 1" > /dev/null 2>&1
do
	sleep 1
	echo -n .
done
echo

psql -h postgres -U postgres -c "CREATE USER torodb WITH SUPERUSER PASSWORD '$TOROPASS'"
psql -h postgres -U postgres -c "CREATE DATABASE torod OWNER torodb"

rm ~/.pgpass