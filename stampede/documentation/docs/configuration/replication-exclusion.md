<h1>Configuring Replication Exclusion</h1>

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
