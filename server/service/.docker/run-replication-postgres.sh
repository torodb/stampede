#!/bin/bash

./setup-postgres.sh
./setup-replication.sh

./toro_dist/bin/torodb -c ./torodb-replication-postgres.yml -l
./toro_dist/bin/torodb -c ./torodb-replication-postgres.yml
