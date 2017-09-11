<h1>Installation Prerequisites</h1>

ToroDB Stampede's correct operation depends on a number of known dependencies, in the next table more information on how to install and manage them is provided.

## Runtime Dependencies

| | Description | External links |
|-|-------------|----------------|
| MongoDB | To act as replication source. | [more info](https://docs.mongodb.com/manual/installation/) |
| Replica set configuration | ToroDB Stampede receives data from a MongoDB replica set. A single-node replica set is sufficient. | [more info](https://docs.mongodb.com/manual/tutorial/deploy-replica-set/) | 
| PostgreSQL | The relational backend to store the normalized data in. | [more info](https://wiki.postgresql.org/wiki/Detailed_installation_guides) |
| Java | ToroDB Stampede is written in Java so a Java Runtime Environmen (JRE) required to run it. | [more info](https://java.com/en/download/help/index_installing.xml) |

## PostgreSQL Configuration

The ToroDB Stampede default configuration expects a new PostgreSQL user named `torodb` and a new PostgreSQL database named `torod`. You can [configure other names](/configuration/postgresql-connectivity.md), of course. The following examples demo creating a user and database that matches the ToroDB Stampede default configuration.

### Linux

```no-highlight
createuser -S -R -D -P --interactive torodb

createdatabase -O torodb torod
```

### macOS/Windows

In macOS and Windows the user and database can be created using an administration connection with `psql` command (do not forget to change `<password>` with the chosen passowrd).

```no-highlight
CREATE USER torodb WITH PASSWORD '<password>';

CREATE DATABASE torod OWNER torodb;
```

## Creating a `.toropass` File

ToroDB Stampede reads the database credentials (user name and password) from the file `.toropass` in the home directory ([configurable via the `toropassFile` setting](/configuration/options-reference.md#postgresql-configuration)).

The following example configures ToroDB Stampede to connect to a PostgreSQL database running on the same machine (localhost) at the default port 5432. The remaining settings are the database, username and password respectively.


!!! warning
    Whoever can read the `.toropass` file can access the SQL backend. Make sure the access rights are set properly on the `.toropass` file.

### Format of `.toropass` File

Create `.toropass` file in the home path with the content below (do not forget to change `<password>` with the chosen passowrd).

```no-highlight
localhost:5432:torod:torodb:<password>
```

### Create `.toropass` File via Script on Linux/macOS

```no-highlight
read -s -p "Enter password:" PASSWORD
echo
echo "localhost:5432:torod:torodb:$PASSWORD" > "$HOME/.toropass"
chmod 0400 "$HOME/.toropass"
```

### Create `.toropass` File via Script on Windows

```no-highlight
set PASSWORD=<password>
echo localhost:5432:torod:torodb:%PASSWORD%>%HOMEDRIVE%%HOMEPATH%\.toropass
```
