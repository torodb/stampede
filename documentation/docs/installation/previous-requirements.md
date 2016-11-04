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

The default installation of ToroDB Stampede requires new user and database to work. This values can be changed and specified by configuration, but here the default specification is explained.

### Linux

```no-highlight
$ createuser -S -R -D -P --interactive torodb

$ createdatabase -O torodb torod
```

### macOS/Windows

In macOS and Windows the user and database can be created using an administration connection with `psql` command.

```no-highlight
> CREATE USER torodb WITH PASSWORD '<password>';

> CREATE DATABASE torod OWNER torodb;
```

## Create .toropass file

The access configuration to the PostgreSQL database will be detailed in the `.toropass` file stored in the home directory. The example assumes local connection with default port is being used, but it can be changed by the user too.

### Linux/macOS

Create `.toropass` file in the home path with the content below.

```no-highlight
localhost:5432:torod:torodb:<password>
```

### Windows

```no-highlight
> set /p pwd="Type torodb user's password:" & cls

> echo localhost:5432:*:postgres:%pwd%> "%HOMEDRIVE%%HOMEPATH%\.toropass"
```
