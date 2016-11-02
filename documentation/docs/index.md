# What is ToroDB Stampede?

Connected to a MongoDB replica set, ToroDB Stampede is able to replicate the NoSQL data into a relational backend (right now the only available backend is PostgreSQL) using the oplog.

![ToroDB Stampede Structure](images/toro_stampede_structure.jpg)

There are other solutions that are able to store the JSON document in a relational table using PostgreSQL JSON support, but it doesn't solve the real problem of 'how to really use that data'. ToroDB Stampede replicates the document structure in different relational tables and stores the document data in different tuples using those tables.

![Mapping example](images/toro_stampede_mapping.jpg)

With the relational structure, some given problems from NoSQL solutions are easier to solve, such as aggregated query execution in an admissible time.

# ToroDB Stampede limitations

Not everything could be perfect and there are some known limitations from ToroDB Stampede.

* The only current MongoDB version supported is 3.2.
* [Capped collections](https://docs.mongodb.com/manual/core/capped-collections/) usage is not supported.
* If character `\0` is used in a string it will be escaped because PostgreSQL doesn't support it.
* Command `applyOps` reception will stop the replication server.
* Command `collmod` reception will be ignored.
* MongoDB sharding environment are not supported currently.

In addition to the previous limitations, just some kind of indexes are supported:

* Index of type ascending and descending (those that ends in 1 and -1 when declared in mongo)
* Simple indexes of one key
* Compound indexes of multiple keys with the exception of unique compound indexes that have keys touching more than a single subdocument (eg: db.test.createIndex({"a": 1, "a.b": 1},{unique: true})
* All keys path with the exception to the paths resolving in scalar value (eg: db.test.createIndex("a": 1) will not index value a of document {"a": [1,2,3]})

[TODO]: <> (not supported types, we need a list)

[Versions]: <> (this section doesn't make any sense currently)

[Documentation conventions]: <> (we have no time right now for this section)

# Glossary

| Term | Definition |
|------|------------|
| __capped collection__ | A fixed-sized collection that automatically overwrites its oldest entries when it reaches its maximum size. |
| __oplog__ | A capped collection that stores an ordered history of logical writes to a MongoDB database. The oplog is the basic mechanism enabling replication in MongoDB.|
| __path__ | (JSON document path) The required sequence of keys to access one specific value of the document (for example, address.zipcode) | 
| __replica set__ | A replica set is the way provided by MongoDB to add redundancy to survive network partitions and other technical failures ([more info](https://docs.mongodb.com/manual/tutorial/deploy-replica-set/)) |
| __sharding__ | When the amount of data is too large it is recommended to distribute the information across multiple machines, this is known as sharding in MongoDB ([more info](https://docs.mongodb.com/v3.2/sharding/)) |
