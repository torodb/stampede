<h1>Quickstart</h1>

This guides provides a step-by-step procedure through your very first ToroDB Stampede sandbox installation. It also covers installation and dummy configuration of MongoDB and PostgreSQL. The sole purpose of this guide is you give you a quick way to play around with ToroDB Stampede. For other purposes, read the [installation](installation/prerequisites.md) and [configuration](configuration/index.md) chapters.


## Prerequisites

ToroDB Stampede requires a Java 8 runtime environment.

## MongoDB

The following is a short step-by-step guide to install MongoDB with a single-node replica set. In case of doubt, refer to the [MongoDB installation guide](https://docs.mongodb.com/manual/administration/install-community/) and the [rs.initiate command](https://docs.mongodb.com/manual/reference/method/rs.initiate/) for further details.

1. Download the latest [MongoDB community server](https://www.mongodb.com/download-center#community).
1. Create a directory for the MongoDB data files (e.g., `/tmp/mongo/`).
1. Start MongoDB (`mongod` or `mongod.exe`) with the following parameters:  
    `--dbpath /tmp/mongo/ --replSet rs1`  
    **Note:** `rs1` is the default replication set name expected by ToroDB Stampede. 

By now, you should now have a running MongoDB listening on localhost port 27017 without access control. This allows the MongoDB shell as well as ToroDB Stampede to connect without any arguments.

To configure a single-node replica set (named `rs1`), start the MongoDB shell and run the following command in the just started MongoDB server:

```
rs.initiate(
   {
      _id: "rs1",
      version: 1,
      members: [
         {
            _id: 0,
            host: "127.0.0.1"
         }
      ]
   }
);
```

## PostgreSQL

 
Follow the instructions on the [PostgreSQL downloads](https://www.postgresql.org/download/) page to download and install PostgreSQL.

Once PostgreSQL is running create a user and database for ToroDB Stempede:

1. Use PostgreSQL's `createuser` to create the user `torodb`.  
   On Linux, this can be done with the following command:  
    `sudo -u postgres createuser -S -R -D -P --interactive torodb`  
   You will be asked for a password, which you'll need to tell ToroDB Stampede in a moment.
1. Create database `torod` with owner `torodb`.  
   On Linux, this can be done with the following command:  
    `sudo -u postgres createdb -O torodb torod`
1. Verify the connectivity.  
   On Linux, this can be done with the following command:  
    `psql -U torodb torod`

## ToroDB Stampede

Download the latest ToroDB Stampede **binary distribution** from the [downloads page](https://www.torodb.com/stampede/download) and start it with the `--ask-for-password` option.

On Linux, this can be done with the following commands:

```no-highlight
wget "https://www.torodb.com/download/torodb-stampede-1.0.0-beta1.tar.bz2"

tar xjf torodb-stampede-1.0.0-beta1.tar.bz2

torodb-stampede-1.0.0-beta1/bin/torodb-stampede --ask-for-password
```

ToroDB Stampede will ask for the password of the `torodb` PostgreSQL user you just created.

If all goes fine, ToroDB Stampede is up and running and replicates the operations done in MongoDB.

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

For more detailed explanation about JSON document mapping to tables in a relational database, read [The Relational Schema](relational-schema.md) documentation chapter.
