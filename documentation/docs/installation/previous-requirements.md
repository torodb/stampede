<h1>Previous requirements</h1>

## Project dependencies

ToroDB Stampede's correct operation depends on a number of known dependencies, in the next table more information on how to install and manage them is provided.

| | Description | External links |
|-|-------------|----------------|
| MongoDB | It is the NoSQL system where the original data is stored and the replication data source. | [more info](https://docs.mongodb.com/manual/installation/) |
| Replica set configuration | ToroDB Stampede is designed to replicate from a MongoDB replica set, so it should be previously configured. | [more info](https://docs.mongodb.com/manual/tutorial/deploy-replica-set/) | 
| PostgreSQL | ToroDB Stampede correct operation relies on the existence of a backend, right now it should be PostgreSQL. | [more info](https://wiki.postgresql.org/wiki/Detailed_installation_guides) |
| Java | ToroDB Stampede has been written in Java so a Java Virtual Machine is required for it's execution. | [more info](https://java.com/en/download/help/index_installing.xml) |

## Backend setup

### PostgreSQL configuration

To work properly, the default installation of ToroDB Stampede requires a new user and a new database. User and database can be custom and specified in the configuration, but here we will explain how to create the user and database to work with default configuration.

#### Linux

```no-highlight
createuser -S -R -D -P --interactive torodb

createdatabase -O torodb torod
```

#### macOS/Windows

In macOS and Windows the user and database can be created using an administration connection with `psql` command (do not forget to change `<password>` with the chosen passowrd).

```no-highlight
CREATE USER torodb WITH PASSWORD '<password>';

CREATE DATABASE torod OWNER torodb;
```

### Create .toropass file

The access configuration to the PostgreSQL database will be detailed in the `.toropass` file stored in the home directory. 
The example assumes local connection with default port is being used, but it can be changed by the user too.

Create `.toropass` file in the home path with the content below (do not forget to change `<password>` with the chosen passowrd).

```no-highlight
localhost:5432:torod:torodb:<password>
```

#### Linux/macOS

```no-highlight
read -s -p "Enter password:" PASSWORD
echo
echo "localhost:5432:torod:torodb:$PASSWORD" > "$HOME/.toropass"
```

#### Windows

```no-highlight
set PASSWORD=<password>
echo localhost:5432:torod:torodb:%PASSWORD%>%HOMEDRIVE%%HOMEPATH%\.toropass
```
