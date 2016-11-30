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
```

You may change this configuration depending on your requisites.
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
```

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

## Exclude replication

By default ToroDB Stampede replicates all databases and collections available in your MongoDB. You can exclude a whole database or some collection changing ToroDB Stampede configuration.

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

For example, if you have two databases, *films* and *music*, and each one has two collections, *title* and *performer*. The configuration to exclude the whole *music* database, but in *film* database only *performer* collection, you should write:  

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
	If you stop ToroDB Stampede, remove an exclusion, and restart ToroDB Stampede, the replication process will replicate operations on this database/collection without replicating previously data form this database/collection, reaching an inconsistent state.
	
	It is recommended to delete ToroDB Stampede database and restart the whole replication process from scratch.     

### Exclude a MongoDB index

Some index created in MongoDB for OLTP operations can be useless for OLAP and analytics operations. MongoDB indexes can be excluded in ToroDB Stampede allowing you to save disk space. You just need to add the index name  in the exclude section.

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
	If you stop ToroDB Stampede, remove an exclusion, and restart ToroDB Stampede, the replication process will not create the previously excluded indexes. ToroDB Stampede only creates indexes at the initial recovery process and when a create index command is found in the oplog replication process.
