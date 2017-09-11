<h1>Quickstart</h1>

This guides provides a step-by-step procedure through your very first ToroDB Stampede sandbox installation. It also covers installation and dummy configuration of MongoDB and PostgreSQL. The sole purpose of this guide is to give ToroDB Stampede a quick try. For other purposes, read the [installation](installation/prerequisites.md) and [configuration](configuration/index.md) chapters.

__In case of any problems, consult the [Trouble Shooting Guide](trouble-shooting.md)__

## Prerequisites

ToroDB Stampede requires a Java 8 runtime environment.

## MongoDB

The following is a short step-by-step guide to install MongoDB with a single-node replica set. In case of doubt, refer to the [MongoDB installation guide](https://docs.mongodb.com/manual/administration/install-community/) and the [rs.initiate command](https://docs.mongodb.com/manual/reference/method/rs.initiate/) for further details.

1. Download [MongoDB 3.4 community server](https://www.mongodb.com/download-center).  
1. Create a directory for the MongoDB data files (e.g., `/tmp/mongo/`).
1. Start MongoDB (`mongod` or `mongod.exe`) with the following parameters:  
    `--dbpath /tmp/mongo/ --replSet rs1`  
    **Note:** `rs1` is the default replication set name expected by ToroDB Stampede.

By now, you should have a running MongoDB listening on localhost port 27017 without access control. This allows the MongoDB shell as well as ToroDB Stampede to connect without any arguments.

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
wget "https://www.torodb.com/download/torodb-stampede-latest.tar.bz2"

tar xjf torodb-stampede-*.tar.bz2

torodb-stampede-*/bin/torodb-stampede --ask-for-password
```

ToroDB Stampede will ask for the password of the `torodb` PostgreSQL user you just created.

By now, you should have running sandbox environment. Changes done in MongoDB are replicated to PostgreSQL and stored in the [relational schema](relational-schema.md). The following sections demos ToroDB Stampede by uploading test data into MongoDB and runing a very simple query against this data in the PostgreSQL database.

## Uploading Test Data

You can import the "primer dataset" into MongoDB as described in the [MongoDB manual](https://docs.mongodb.com/getting-started/shell/import-data/):

On Linux, the following commands will do the trick:

```no-highlight
wget https://www.torodb.com/download/primer-dataset.json

mongoimport --db test --collection restaurants primer-dataset.json
```
Note that the MongoDB database name (`test`) is mapped to a PostgreSQL **schema**. You can either connect with your favourite GUI to PostgreSQL or use `psql` on the command line:

```no-highlight
psql -U torodb torod

> set schema 'test'
```

To view the table created by ToroDB Stampede, use the `\d` command in `psql`:

```no-highlight
torod=> \d
                  List of relations
 Schema |           Name            | Type  | Owner
--------+---------------------------+-------+--------
 test   | restaurants               | table | torodb
 test   | restaurants_address       | table | torodb
 test   | restaurants_address_coord | table | torodb
 test   | restaurants_grades        | table | torodb
(4 rows)
```

The chapter [Relational Schema](relational-schema.md) explains how JSON documents are mapped to tables.

## Example Query

To list each ZIP code with the number of restaurants in order of decending number of restaurants:

```no-highlight
select zipcode_s, count(*)
  from restaurants_address
 group by zipcode_s
 order by count(*) desc;
```

```no-highlight
 count | zipcode_s
-------+-----------
   686 | 10003
   675 | 10019
   611 | 10036
   520 | 10001
   485 | 10022
[...]
```



The equivalent MongoDB query would be:


```no-highlight
db.restaurants.aggregate(
    [
        { $group: {"_id": "$address.zipcode", count:{ $sum:1} } },
        { $sort:  { count: -1 } }
    ]
);
```

```no-highlight
{ "_id" : "10003", "count" : 686 }
{ "_id" : "10019", "count" : 675 }
{ "_id" : "10036", "count" : 611 }
{ "_id" : "10001", "count" : 520 }
{ "_id" : "10022", "count" : 485 }
[...]
```

[TODO]: <> (Change the name of the dataset (aka "primer dataset") to not expose spanish names)