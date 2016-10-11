#!/bin/bash

set -e

echo -n "Waiting MongoDB..."
max_seconds=15
while ! /opt/mongodb/bin/mongo mongo:27017/local --eval true > /dev/null 2>&1
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

echo -n "Setting up MongoDB..."
/opt/mongodb/bin/mongo  mongo:27017/local --eval 'rs.initiate({_id:"rs1",members:[{_id:0,host:"localhost:27017"}]})' > /dev/null 2>&1
max_seconds=15
while ! /opt/mongodb/bin/mongo mongo:27017/local --eval 'assert(db.isMaster().ismaster)' > /dev/null 2>&1
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
