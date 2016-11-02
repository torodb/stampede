# Previous requirements installation

ToroDB Stampede's correct operation depends on a number of prerequisites, in the next table more information on how to install and manage them is provided.

| | Decription | External links |
|-|------------|----------------|
| MongoDB | It is the NoSQL system where the original data is stored and the replication data source. | [more info](https://docs.mongodb.com/manual/installation/) |
| Replica set configuration | ToroDB Stampede is designed to replicate from a MongoDB replica set, so it should be previously configured. | [more info](https://docs.mongodb.com/manual/tutorial/deploy-replica-set/) | 
| PostgreSQL | ToroDB Stampede correct operation relies on the existence of a backend, right now it should be PostgreSQL. | [more info](https://wiki.postgresql.org/wiki/Detailed_installation_guides) |
| Java | ToroDB Stampede has been written in Java so a Java Virtual Machine is required for it's execution. | [more info](https://java.com/en/download/help/index_installing.xml) |

Among the previous prerequisites, if we want to compile the source code other requisites are mandatory.

| | Decription | External links |
|-|------------|----------------|
| Maven | dependency management and construction tasks has been delegated to Apache Maven, so it is necessary to compile the source code. | [more info](http://maven.apache.org/install.html) | 

# Backend setup

## PostgreSQL configuration

ToroDB Stampede need a user and a database to be created in PostgreSQL to connect and store all the replicated data.

### Linux

Create PosgreSQL user torodb:

    createuser -S -R -D -P --interactive torodb

Create PostgreSQL database torod with owner torodb:

    createdatabase -O torodb torod

### Mac OS X/Windows

Open a console running `psql` command and type:

    CREATE USER torodb WITH PASSWORD '<torodb user''s password>';
    CREATE DATABASE torod OWNER torodb;

## Create .toropass file

Assuming that PostgreSQL is running on host localhost and port 5432:

### Linux/Mac OS X

Create a file that will contain the PostgreSQL user torodb's password:

    echo "localhost:5432:torod:torodb:$(\
        read -p "Type torodb user's password:"$'\n' -s pwd; echo $pwd)" > $HOME/.toropass

### Windows

    set /p pwd="Type torodb user's password:" & cls
    echo localhost:5432:*:postgres:%pwd%> "%HOMEDRIVE%%HOMEPATH%\.toropass"

# Executing ToroDB Stampede

## Docker

ToroDB Stampede can be tested in a Docker container in two different ways. First option is to download the containers from a repository, the second one is to launch with Maven Docker tasks.

### From Docker repository

#### With docker image

    docker run -ti -v `realpath <postgres-credentials-file>`:/root/.toropass 8kdata/torodb-stampede

or

    TORODB_BACKEND_PASSWORD="<torodb user's password>" docker run -ti 8kdata/torodb-stampede


#### With docker-compose

Download the docker compose file and run it:

    wget http://todo

    docker-compose up

### From source code

#### Linux/Mac

The source code contains some Maven tasks that can build the right artifacts to execute ToroDB Stampede and its dependencies in Docker containers. It can be done with the next commands.

    $ mvn clean package -P docker -Ddocker.skipbase=false
    
    $ mvn -f packaging/stampede/main/pom.xml -P docker-stampede-fullstack docker:run -Ddocker.follow

Some errors can appear due to the Docker cache, if that happens then cache can be disabled using command options, like is done in the next example. Usually these errors are related to network connection timeouts.

    $  mvn clean package -P docker -Ddocker.skipbase=false -Ddocker.nocache=true

## Binary distribution

One recommended way to use ToroDB Stampede is through the binary distribution. It means that one precompiled distribution is downloaded and then executed using command tools.

### Linux/Mac

Given that previous prerequisites are met, the only step needed to launch ToroDB Stampede is the download of the binary distribution from the next [link](http://todo).

    $ wget http://todo
    
    $ tar xjvf <stampede-binary>.tar.bz2
    
    $ torodb-stampede-<version>/bin/torodb-stampede

The main problems at this step is that MongoDB or PostgreSQL has a different user/password than expected, to avoid that problem configuration files can be provided.

    $ torodb-stampede-<version>/bin/torodb-stampede --toropass-file <postgres-credentials-file> --mongopass-file <mongo-credentials-file>

### As a Linux systemd service

All the following commands must be run as root. Precede them by sudo command if you are running in Ubuntu.

First you have to create a symbolic link to `bin/torodb-stampede` in `/usr/bin` folder:

    ln -s "`pwd`/bin/torodb-stampede" /usr/bin/.

You have to create the system user `torodb`:

    useradd -M -d "$(dirname "$(dirname "`pwd`/bin/torodb-stampede")")" torodb

Now copy the file `systemd/torodb-stampede.service.sample` to `/lib/systed/system` folder:

    cp systemd/torodb-stampede.service.sample /lib/systed/system/.

Enable and start the newly created ToroDB Stampede service:

    systemctl enable torodb-stampede
    systemctl start torodb-stampede

To view logs of ToroDB Stampede service:

    journalctl --no-pager -u torodb-stampede

Following logs:

    journalctl --no-pager -u torodb-stampede -f

View all logs:

    journalctl --no-tail --no-pager -u torodb-stampede

To stop ToroDB Stampede service:

    systemctl stop torodb-stampede


## Source code

# Basic configuration

## Config file

## CLI parameters

# Advanced configuration

## Config file

## CLI parameters
