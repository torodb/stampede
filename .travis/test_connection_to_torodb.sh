#!/bin/bash

set -ev

# Wait until torodb it's ready (or timeout after 10s)
COUNTER=10
while ! nc -z localhost 27019 && [ $COUNTER -gt 0 ]
do
	echo "waiting... ($COUNTER seconds left)"
	sleep 1
	COUNTER=$((COUNTER-1))
done
echo

mongo --host localhost --port 27019 --eval "quit()"
