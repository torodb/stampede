# Previous requirements

ToroDB Stampede correct operation has a few requirement.

* A proper replica set configuration of MongoDB.
* One PostgreSQL instance to be used as a relational backend.
* Java 8 virtual machine.

If one or more of these requirements are not meet, more information and additional documentation can be found in the [installation](installation.md) chapter.

# Backend configuration

ToroDB Stampede expects some base configuration from the relational backend. So, if PostgreSQL is correctly installed the next steps should be done.

* Create role `torodb` with permissions to create a database and be able to do login.
* Create database `torod` with owner `torodb`.

This steps can be done with the next commands in a Linux environment.

```
$ sudo -u postgres createuser -S -R -D -P --interactive torodb

$ sudo -u postgres createdb -O torodb torod

$ sudo adduser torodb
```

The easiest way to check if the database can be used is connecting to it using the new role. If it is accessible then ToroDB Stampede should be able to do replication using it.

```
$ sudo -u torodb psql torod
```

# How to execute ToroDB Stampede binary distribution?

To execute ToroDB Stampede the binary distribution is necessary and it can be downloaded from  [here](https://www.dropbox.com/s/54eyp7jyu8l70aa/torodb-stampede-0.50.0-SNAPSHOT.tar.bz2?dl=0). After download and when file is uncompressed then ToroDB Stampede can be launched using the PostgreSQL connection information.

Create a PostgreSQL credentials configuration file, using the `.pgpass` file structure. The right format is one or more lines formatted as `<host>:<port>:<database>:<user>:<password>`.

```
localhost:5432:torod:torodb:torodb
```

Once the credentials file is created ToroDB Stampede can be launched.

```
$ wget https://www.dropbox.com/s/54eyp7jyu8l70aa/torodb-stampede-0.50.0-SNAPSHOT.tar.bz2?dl=0

$ tar xjvf <stampede-binary>.tar.bz2

$ torodb-stampede-<version>/bin/torodb-stampede --toropass-file <postgres-credentials>
```

If all goes fine, ToroDB Stampede is up and running and it will be replicating the operations done in MongoDB.

# Replication example

It is easier to understand what ToroDB Stampede does through an example. One dataset will be imported in MongoDB and all data will be available in PostgreSQL thanks to Stampede replication.

If previous steps are done and ToroDB Stampede is up and running, the dataset can be downloaded from  [here](https://www.dropbox.com/s/570d4tyt4hpsn03/primer-dataset.json?dl=0) and the replication done using `mongoimport` command.

```
$ wget https://www.dropbox.com/s/570d4tyt4hpsn03/primer-dataset.json?dl=0

$ mongoimport -d stampede -c primer primer-dataset.json
```

When `mongoimport` finished PostgreSQL should have the replicated structure and data stored in the `stampede` schema, because that was the name selected for the database in the `mongoimport` command. Connecting to PostgreSQL console, the data can be accessed.

```
$ sudo -u torodb psql torod

# set schema 'stampede'
```

The next table structure in the `stampede` schema is created.

```
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
