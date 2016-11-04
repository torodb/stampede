# Project dependencies

ToroDB Stampede's correct operation depends on a number of known dependencies, in the next table more information on how to install and manage them is provided.

| | Description | External links |
|-|-------------|----------------|
| MongoDB | It is the NoSQL system where the original data is stored and the replication data source. | [more info](https://docs.mongodb.com/manual/installation/) |
| Replica set configuration | ToroDB Stampede is designed to replicate from a MongoDB replica set, so it should be previously configured. | [more info](https://docs.mongodb.com/manual/tutorial/deploy-replica-set/) | 
| PostgreSQL | ToroDB Stampede correct operation relies on the existence of a backend, right now it should be PostgreSQL. | [more info](https://wiki.postgresql.org/wiki/Detailed_installation_guides) |
| Java | ToroDB Stampede has been written in Java so a Java Virtual Machine is required for it's execution. | [more info](https://java.com/en/download/help/index_installing.xml) |

Among the previous dependencies, if we want to compile the source code other requisites are mandatory.

| | Description | External links |
|-|-------------|----------------|
| Git | It is the distributed version control system (DVCS) used to keep ToroDB Stampede source code up to date and synchronized between its committers. | [more info](https://git-scm.com/downloads) |
| Maven | Dependency management and construction tasks has been delegated to Apache Maven, so it is necessary to compile the source code. | [more info](http://maven.apache.org/install.html) | 

# Backend setup

## PostgreSQL configuration

ToroDB Stampede need a user and a database to be created in PostgreSQL to connect and store all the replicated data.

### Linux

Create PosgreSQL user torodb:

```
$ createuser -S -R -D -P --interactive torodb
```

Create PostgreSQL database torod with owner torodb:

```
$ createdatabase -O torodb torod
```

### macOS/Windows

Open a console running `psql` command and type:

```
# CREATE USER torodb WITH PASSWORD '<torodb user''s password>';

# CREATE DATABASE torod OWNER torodb;
```

## Create .toropass file

Assuming that PostgreSQL is running on host localhost and port 5432:

### Linux/macOS

Create a file that will contain the PostgreSQL user torodb's password:

```
$ echo "localhost:5432:torod:torodb:$(\
  read -p "Type torodb user's password:"$'\n' -s pwd; echo $pwd)" > $HOME/.toropass
```

### Windows

```
> set /p pwd="Type torodb user's password:" & cls

> echo localhost:5432:*:postgres:%pwd%> "%HOMEDRIVE%%HOMEPATH%\.toropass"
```
