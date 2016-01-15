#!/bin/bash

set -ev

function wait_mongo_on_port {
	# Wait until mongo logs that it's ready (or timeout after 30s)
	COUNTER=0
	while ! nc -z localhost "$1" && [ "$COUNTER" -lt "30" ]
	do
		echo 
		sleep 2
		COUNTER+=$((COUNTER+2))
	done
}

if [ "$Protocol" == "Mongo" -o "$Protocol" == "MongoReplSet" ]
then
	wget http://downloads.mongodb.org/linux/mongodb-linux-x86_64-ubuntu1204-3.0.8.tgz -O /tmp/mongodb.tgz
	tar xvf /tmp/mongodb.tgz
	export PATH=$(ls -d $PWD/mongodb-linux-x86_64-ubuntu1204-3.0*)/bin:$PATH
	
	if [ "$Protocol" == "MongoReplSet" ]
	then
		mkdir -p /tmp/mongodb/rs1-0
		mongod --port 27020 --dbpath /tmp/mongodb/rs1-0 --replSet rs1 --smallfiles --oplogSize 128 &
		wait_mongo_on_port 27020
		mongo --host localhost --port 27020 --eval 'rs.initiate({_id:"rs1",members:[{_id:0,host:"localhost:27020"}]})'
	fi
fi
