#!/bin/bash

./setup-postgres.sh

./toro_dist/bin/torodb -c ./torodb-postgres.yml -l
./toro_dist/bin/torodb -c ./torodb-postgres.yml
