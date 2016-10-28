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

# Executing ToroDB Stampede

## Docker

ToroDB Stampede can be tested in a Docker container in two different ways. First option is to download the containers from a repository, the second one is to launch with Maven Docker tasks.

### From Docker repository

### From source code

#### Linux/Mac

The source code contains some Maven tasks that can build the right artifacts to execute ToroDB Stampede and its dependencies in Docker containers. It can be done with the next commands.

```
$ mvn clean package -P docker -Ddocker.skipbase=false

$ mvn -f packaging/stampede/main/pom.xml -P docker-stampede-fullstack docker:run -Ddocker.follow
```

Some errors can appear due to the Docker cache, if that happens then cache can be disabled using command options, like is done in the next example. Usually these errors are related to network connection timeouts.

```
$  mvn clean package -P docker -Ddocker.skipbase=false -Ddocker.nocache=true
```

## Binary distribution

One recommended way to use ToroDB Stampede is through the binary distribution. It means that one precompiled distribution is downloaded and then executed using command tools.

### Linux/Mac

Given that previous prerequisites are met, the only step needed to launch ToroDB Stampede is the download of the binary distribution from the next [link](http://todo).

```
$ wget http://todo

$ tar xjvf <stampede-binary>.tar.bz2

$ torodb-stampede-<version>/bin/torodb-stampede
```

The main problems at this step is that MongoDB or PostgreSQL has a different user/password than expected, to avoid that problem configuration files can be provided.

```
$ torodb-stampede-<version>/bin/torodb-stampede --toropass-file <postgres-credentials-file> --mongopass-file <mongo-credentials-file>
```

## Source code

# Basic configuration

## Config file

## CLI parameters

# Advanced configuration

## Config file

## CLI parameters
