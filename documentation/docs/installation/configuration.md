<h1>Configuration</h1>

ToroDB Stampede can be launch with custom configuration options. There are two ways to do it, using command modifiers or using a configuration file. The recommended way is using a configuration file because it is more versatile and self-documented.

To use the configuration file, the `-c` parameter should be specified.

```no-highlight
torodb-stampede -c myconfiguration.yml

```

Also you can check configuration used by ToroDB Stampede using the `-l` parameter.

```no-highlight
torodb-stampede -l
```

The previous sections talk about basic configuration of the system, but it is highly probable that some specific configuration must be done to work in production environments.

## Custom PostgreSQL connection

By default ToroDB Stampede connects to PostgreSQL using the following configuration:

```json
backend:
  postgres:
    host: localhost
    port: 5432
    database: torod
    user: torodb
    toropassFile: "~/.toropass"
    applicationName: "toro"
    ssl: false
```

You may change this configuration depending on your requisites.
You can enabled SSL connection setting `ssl: true` in configuration file.  
To provide the PostgreSQL user's password that ToroDB Stampede will use to connect to PostgreSQL 
you can specify parameter `--ask-for-password` to make ToroDB Stampede prompt for the password while starting up  
or you create a PostgreSQL credentials configuration file `~/.toropass`, using the `.pgpass` file format. 
The right format is one or more lines formatted as `<host>:<port>:<database>:<user>:<password>`.

```no-highlight
echo "localhost:5432:torod:torodb:torodb" > ~/.toropass
chmod 400 ~/.toropass
```

You may change the `.toropass` path using the `toropassFile` parameter in ToroDB Stampede configuration file. For example:

```json
backend:
  postgres:
    host: localhost
    port: 5432
    database: torod
    user: torodb
    toropassFile: /secret/mytoropass
    applicationName: "toro"
    ssl: false
```
## Backend connection pooling

By default ToroDB Stampede uses a connection pool with the following configuration:

```json
backend:
  pool:
    connectionPoolTimeout: 10000
    connectionPoolSize: 30
```

You may tune those parameters at will. The only constraint is that `connectionPoolSize` has to be at least 20.

## Custom MongoDB connection

ToroDB Stampede will connect to MongoDB using no authentication and no SSL connection by default. You can set up the connection to MongoDB using `auth` and `ssl` sections in ToroDB Stampede configuration.

For example to connect using cr or scram_sha1 authentication mode with simple SSL support you may use following configuration:

```json
replication:
  replSetName: rs1
  syncSource: localhost:27017
  auth:
    mode: negotiate
    user: mymongouser
    source: mymongosource
  ssl:
    enabled: true
    allowInvalidHostnames: false
    caFile: mycafile.pem
```

## Filtering replication

By default ToroDB Stampede replicates all databases and collections available in your MongoDB. 
You can specify some filters that allow to include a single database, include only some collections, 
exclude a whole database or exclude some collections changing ToroDB Stampede configuration.
Exclusions always override inclusions so that if you exclude something it will prevail over an inclusion.

!!! info
    Lets assume for our examples that you have two databases, *films* and *music*, and each one has two collections, *title* and *performer*. 

### Include only a MongoDB database or collection

In the replication section of the yml config file add an include item with the database to include:

```json
replication:
  replSetName: rs1
  syncSource: localhost:27017
  include: 
    <database name>: "*"
```

or if you want to include just some collections:

```json
replication:
  replSetName: rs1
  syncSource: localhost:27017
  include: 
    <database name>:
      - <collection name 1>
      - <collection name 2>
```

If you want to include only the database called *film* but not the specific collection *performer* from same *film* database, you should write: 

```json
replication:
  replSetName: rs1
  syncSource: localhost:27017
  include:
    film: "performer"
```

!!! danger "Inclusion removal"
    If you stop ToroDB Stampede, remove an inclusion, and restart ToroDB Stampede, the replication process will replicate operations on this database/collection
     without replicating previously data form not included database/collection, reaching an inconsistent state.
    
    It is recommended to delete ToroDB Stampede database and restart the whole replication process from scratch.

### Include only a MongoDB collection and a specific index inside that collection

Sometimes you may want be sure that only specific indexes created in MongoDB have to be replicated by ToroDB Stampede. 
MongoDB indexes can be included in ToroDB Stampede allowing you to save disk space and remove unuseful indexes. You just need to add the index name in the include section.

```json
replication:
  replSetName: rs1
  syncSource: localhost:27017
  include: 
    <database name>:
      <collection name>:
        - name: <index name>
```

If you want to include only collection *performer* from *film* database with the index called *city*, you should write: 

```json
replication:
  replSetName: rs1
  syncSource: localhost:27017
  include: 
    film: 
      performer:
        - name: "city"
```

!!! danger "Inclusion removal"
    If you stop ToroDB Stampede, remove an inclusion, and restart ToroDB Stampede, the replication process will not create the previously not included indexes. 
    ToroDB Stampede only creates indexes at the initial recovery process and when a create index command is found in the oplog replication process.

### Exclude a MongoDB database or collection

In the replication section of the yml config file add an exclude item with the database to exclude:   

```json
replication:
  replSetName: rs1
  syncSource: localhost:27017
  exclude: 
    <database name>: "*"
```

or if you want to exclude just some collections:

```json
replication:
  replSetName: rs1
  syncSource: localhost:27017
  exclude: 
    <database name>:
      - <collection name 1>
      - <collection name 2>
```

The configuration to exclude the whole *music* database, but in *film* database only *performer* collection, you should write:  

```json
replication:
  replSetName: rs1
  syncSource: localhost:27017
  exclude: 
    music: "*"
    film: 
      - performer
```

In this case the only collection replicated is *title* from *film* database.

!!! danger "Exclusion removal"
    If you stop ToroDB Stampede, remove an exclusion, and restart ToroDB Stampede, the replication process will replicate operations on this database/collection
     without replicating previously data form this database/collection, reaching an inconsistent state.
    
    It is recommended to delete ToroDB Stampede database and restart the whole replication process from scratch.

### Exclude a MongoDB index

Some index created in MongoDB for OLTP operations can be useless for OLAP and analytics operations. 
MongoDB indexes can be excluded in ToroDB Stampede allowing you to save disk space. You just need to add the index name  in the exclude section.

```json
replication:
  replSetName: rs1
  syncSource: localhost:27017
  exclude: 
    <database name>:
      <collection name>:
        - name: <index name>
```

If you want to exclude the index called *city* on collection *performer* from *film* database, you should write: 

```json
replication:
  replSetName: rs1
  syncSource: localhost:27017
  exclude: 
    film: 
      performer:
        - name: city
```

Any unsupported index in ToroDB Stampede (text , 2dsphere, 2d, hashed, ...) is ignored and is not created in the relational database, and you don't need to exclude it.

!!! danger "Exclusion removal"
    If you stop ToroDB Stampede, remove an exclusion, and restart ToroDB Stampede, the replication process will not create the previously excluded indexes. 
    ToroDB Stampede only creates indexes at the initial recovery process and when a create index command is found in the oplog replication process.

### Include only a MongoDB database but not a specific collection

You can combine the include and exclude sections to indicate that only a particular database have to be included, but exclude a particular collection in the database.

If you want to include only the database called *film* but not the specific collection *performer* from same *film* database, you should write: 

```json
replication:
  replSetName: rs1
  syncSource: localhost:27017
  include:
    film: "*"
  exclude: 
    film: "performer"
```

### Include only a MongoDB collection in a database but not a specific index inside that collection

You can combine the include and exclude sections to indicate that only a particular collection have to included with all indexes excluding just one.

If you want to include only collection *performer* from *film* database but not the index called *city*, you should write: 

```json
replication:
  replSetName: rs1
  syncSource: localhost:27017
  include: 
    film: 
      performer: "*"
  exclude: 
    film: 
      performer:
        - name: "city"
```

## Replicate from a MongoDB Sharded Cluster


In the replication section of the yml config file add a shards item with the list of shards's configurations, one for each shard:

```json
replication:
  shards:
    - replSetName: shard1
      syncSource: localhost:27020
    - replSetName: shard2
      syncSource: localhost:27030
    - replSetName: shard3
      syncSource: localhost:27040
```
