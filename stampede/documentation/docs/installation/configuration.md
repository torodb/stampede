<h1>Configuration</h1>

ToroDB Stampede can be launch with custom configuration options. There are two ways to do it, using command modifiers or using a configuration file. The recommended way is using a configuration file because it is more versatile and self-documented.

To use the configuration file, the `-c` modifier should be specified.

```

```

The previous sections talk about basic configuration of the system, but it is highly probable that some specific configuration must be done to work in production environments.

## Custom relational database connection

Among the proper configuration into the relational database, ToroDB Stampede requires the URL to the server.

## Custom MongoDB connection

TODO

## Exclude replication

By default ToroDB Stampede replicates all databases and collections available in your MongoDB. You can exclude a whole database or some collection changing ToroDB Stampede configuracion.

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


### Exclude a MongoDB index

Some index created in MongoDB for OLTP operations can be unusefull for OLAP and analytics operations. MongoDB indexes can be excluded in ToroDB Stampede allowing you to save disk space. You just need to add the index name  in the exclude section.

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

!!! danger "Exclusion removal"
	If you stop ToroDB Stampede, remove an exclusion, and restart ToroDB Stampede, the replication process will replicate operations on this database/collection without replicating previously data form this database/collection, reaching an inconsistent state.
	
	It is recommended to delete ToroDB Stampede database and restart the whole replication process from scratch.     
 