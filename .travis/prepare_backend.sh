#!/bin/bash

set -ev

function wait_backend_on_port {
    # Wait until backend it's ready (or timeout after 30s)
    COUNTER=0
    while ! nc -z localhost "$1" && [ "$COUNTER" -lt "30" ]
    do
        echo 
        sleep 2
        COUNTER+=$((COUNTER+2))
    done
}

function wait_mysql_on_socket {
    # Wait until mysql it's ready (or timeout after 30s)
    COUNTER=0
    while ! echo "SELECT 1"|"$mysql_home/bin/mysql" --socket="$1" -u root && [ "$COUNTER" -lt "30" ]
    do
        echo 
        sleep 2
        COUNTER+=$((COUNTER+2))
    done
}

if [ "$Backend" == "Postgres" ]
then
    psql -c "create user torodb with superuser password 'torodb';" -U postgres
fi

if [ "$Backend" == "MySQL" ]
then
    libaio_deb="libaio1_0.3.109-4_amd64.deb"
    libaio_home="libaio"
    
    if [ ! -f "$libaio_deb" ]
    then
        wget "http://launchpadlibrarian.net/139445997/libaio1_0.3.109-4_amd64.deb" -O "$libaio_deb"
    fi
    
    if [ ! -d "$libaio_home" ]
    then
        mkdir "$libaio_home"
        dpkg -x "$libaio_deb" "$libaio_home"
    fi
    
    export LD_LIBRARY_PATH="$PWD/$libaio_home/lib/x86_64-linux-gnu"
    
    mysql_bin="$PWD/mysql.tar.gz"
    mysql_home="$PWD/mysql"
    if [ ! -f "$mysql_bin" ]
    then
        wget "https://dev.mysql.com/get/Downloads/MySQL-5.7/mysql-5.7.11-linux-glibc2.5-x86_64.tar.gz" -O "$mysql_bin"
    fi
    
    if [ ! -d "$mysql_home" ]
    then
        tar xzf "$mysql_bin"
        mv "mysql-5.7.11-linux-glibc2.5-x86_64" "$mysql_home"
    fi
    
    mkdir "$mysql_home/data"
    
    "$mysql_home/bin/mysqld" --no-defaults --initialize-insecure -u "$(whoami)" -h "$mysql_home/data" -b "$mysql_home"
    "$mysql_home/bin/mysqld" --no-defaults --socket="$mysql_home/data/mysql.sock" -u "$(whoami)" -P 3307 --max-connections=50 -h "$mysql_home/data" -b "$mysql_home" &
    wait_backend_on_port 3307
    wait_mysql_on_socket "$mysql_home/data/mysql.sock"
    echo "FLUSH PRIVILEGES; CREATE USER 'torodb'@'localhost' IDENTIFIED BY 'torodb'; GRANT ALL PRIVILEGES ON *.* TO 'torodb'@'localhost' WITH GRANT OPTION; FLUSH PRIVILEGES;"|\
        "$mysql_home/bin/mysql" --socket="$mysql_home/data/mysql.sock" -u root
fi

if [ "$Backend" == "Greenplum" ]
then
	pip install --user psi
	pip install --user lockfile
	greenplum_bin="$PWD/gp.tar.bz2"
	greenplum_home="$PWD/greenplum-db"
	if [ ! -f "$greenplum_bin" ]
	then
		wget "https://github.com/teoincontatto/gpdb/blob/builds/gp.tar.bz2?raw=true" -O "$greenplum_bin"
	fi
    
    if [ ! -d "$greenplum_home" ]
    then
        tar xjf "$greenplum_bin"
		ln -s $PWD/gp /tmp/gp
		ln -s $PWD/gp $greenplum_home
    fi;
	
	echo '
	ARRAY_NAME="GPDB SINGLENODE"
	MACHINE_LIST_FILE='$greenplum_home'/hostlist_singlenode
	SEG_PREFIX=gpsne
	PORT_BASE=40000
	declare -a DATA_DIRECTORY=('$greenplum_home'/gpdata1 '$greenplum_home'/gpdata2)
	MASTER_HOSTNAME='$(hostname)'
	MASTER_DIRECTORY='$greenplum_home'/gpmaster
	MASTER_PORT=6432
	MASTER_SHARED_BUFFERS=2MB
	MASTER_MAX_CONNECT=50
	QE_SHARED_BUFFERS=2MB
	QE_MAX_CONNECT=50
	TRUSTED_SHELL=ssh
	CHECK_POINT_SEGMENTS=8
	ENCODING=UNICODE
	' > $greenplum_home/gpinitsystem_singlenode
	echo $(hostname) > $greenplum_home/hostlist_singlenode
	# configuration to limit shmmax
	echo '
	max_prepared_transactions = 20
	max_locks_per_transaction = 10
	max_appendonly_tables = 100' >> $greenplum_home/share/postgresql/postgresql.conf.sample
	
	mkdir $greenplum_home/gpmaster
	mkdir $greenplum_home/gpdata1
	mkdir $greenplum_home/gpdata2
	
	#travis not accesible commands workaround
	ln -s /bin/echo $greenplum_home/bin/ifconfig
	ln -s /bin/echo $greenplum_home/bin/netstat
	ln -s /bin/echo $greenplum_home/bin/ping
	ln -s /bin/echo $greenplum_home/bin/ping6
	ln -s /bin/echo /tmp/gp/ifconfig
	ln -s /bin/echo /tmp/gp/netstat
	ln -s /bin/echo /tmp/gp/ping
	ln -s /bin/echo /tmp/gp/ping6
	
	#fix path
	sed -i 's#CMDPATH=(/usr/kerberos/bin /usr/sfw/bin /opt/sfw/bin /usr/local/bin /bin /usr/bin /sbin /usr/sbin /usr/ucb /sw/bin)#CMDPATH=('$greenplum_home/bin' /usr/kerberos/bin /usr/sfw/bin /opt/sfw/bin /usr/local/bin /bin /usr/bin /sbin /usr/sbin /usr/ucb /sw/bin)#' $greenplum_home/bin/lib/gp_bash_functions.sh
	#fix ulimit
	sed -i 's-OS_OPENFILES=65535-OS_OPENFILES=64000-' $greenplum_home/bin/lib/gp_bash_functions.sh
	
	source $greenplum_home/greenplum_path.sh
	gpssh-exkeys -f $greenplum_home/hostlist_singlenode
	greenplum_init_config=$greenplum_home/gpinitsystem_singlenode
	if ! gpinitsystem -a -D -c $greenplum_init_config
	then
		echo "==================================================================================="
		echo "================ An error occurred during Greenplum initialization ================"
		echo "==================================================================================="
		echo "full log:"
		cat $HOME/gpAdminLogs/gpinitsystem_*.log
		exit 1
	fi
	
	#permissions
	echo 'host    all             all             127.0.0.1/32            md5' >> $greenplum_home/gpmaster/gpsne-1/pg_hba.conf
	
	export MASTER_DATA_DIRECTORY=$greenplum_home/gpmaster/gpsne-1
    gpstop -a -r
	
	psql -p 6432 template1 -c "CREATE USER torodb WITH SUPERUSER PASSWORD 'torodb'";
fi
