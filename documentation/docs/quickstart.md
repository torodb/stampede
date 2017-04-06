<h1>Quickstart</h1>

## Previous requirements

To work properly, ToroDB Stampede has a few requirement:

* A proper replica set configuration of MongoDB.
* One PostgreSQL instance to be used as a relational backend.
* Java 8 virtual machine.

If one or more of these requirements are not meet, more information and additional documentation can be found in the installation chapters.

## Backend configuration

ToroDB Stampede expects some basic configuration for the relational backend. Then, if PostgreSQL is correctly installed the next steps should be done.

* Create role `torodb` with permissions to create a database and be able to do login.
* Create database `torod` with owner `torodb`.

This steps can be done with the next commands in a Linux environment:

```no-highlight
sudo -u postgres createuser -S -R -D -P --interactive torodb

sudo -u postgres createdb -O torodb torod
```

The easiest way to check if the database can be used is connecting to it using the new role. If it is accessible then ToroDB Stampede should be able to do replication using it.

```no-highlight
psql -U torodb torod
```

## How to execute ToroDB Stampede binary distribution?

To execute ToroDB Stampede the binary distribution is necessary and it can be downloaded from  [here](https://www.torodb.com/download/torodb-stampede-1.0.0-beta2.tar.bz2). After download and when file is uncompressed then ToroDB Stampede can be launched using the PostgreSQL connection information.

Following commands will allow ToroDB Stampede to be launched.

```no-highlight
wget "https://www.torodb.com/download/torodb-stampede-1.0.0-beta2.tar.bz2"

tar xjf torodb-stampede-1.0.0-beta2.tar.bz2

torodb-stampede-1.0.0-beta2/bin/torodb-stampede --ask-for-password
```

ToroDB Stampede will ask for the PostgreSQL torodb user's password to be provided. If all goes fine, ToroDB Stampede is up and running and it will be replicating the operations done in MongoDB.

## Replication example

It is easier to understand what ToroDB Stampede does through an example. One dataset will be imported in MongoDB and all data will be available in PostgreSQL thanks to Stampede replication.

If previous steps are done and ToroDB Stampede is up and running, the dataset can be downloaded from  [here](https://www.torodb.com/download/primer-dataset.json) and the replication done using `mongoimport` command.

```no-highlight
wget https://www.torodb.com/download/primer-dataset.json

mongoimport -d stampede -c primer primer-dataset.json
```

When `mongoimport` finished and replication complete PostgreSQL should have the replicated structure and data stored in the `stampede` schema, because that was the name selected for the database in the `mongoimport` command. Connecting to PostgreSQL console, the data can be accessed.

```no-highlight
sudo -u torodb psql torod

> set schema 'stampede'
```

The next table structure in the `stampede` schema is created.

```no-highlight
torod=# \d
                List of relations
  Schema  |         Name         | Type  | Owner  
----------+----------------------+-------+--------
 stampede | primer               | table | torodb
 stampede | primer_address       | table | torodb
 stampede | primer_address_coord | table | torodb
 stampede | primer_grades        | table | torodb
(4 rows)
```

For more detailed explanation about JSON document mapping to tables in a relational database, read [how to use](how-to-use.md) documentation chapter.
