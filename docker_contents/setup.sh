cp -r $(ls -d /toro/torodb-*-SNAPSHOT)/* /toro_dist
psql -h postgres -U postgres -c "CREATE USER torodb WITH SUPERUSER PASSWORD '$TOROPASS'"
psql -h postgres -U postgres -c "CREATE DATABASE torod OWNER torodb"
