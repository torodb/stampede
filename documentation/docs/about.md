<h1>What is ToroDB Stampede?</h1>

Connected to a MongoDB replica set, ToroDB Stampede is able to replicate the NoSQL data into a relational backend (right now the only available backend is PostgreSQL) using the oplog.

![ToroDB Stampede Structure](images/toro_stampede_structure.jpg)

There are other solutions that are able to store the JSON document in a relational table using PostgreSQL JSON support, but it doesn't solve the real problem of 'how to really use that data'. 
ToroDB Stampede replicates the document structure in different relational tables and stores the document data in different tuples using those tables.

![Mapping example](images/toro_stampede_mapping.jpg)

With the relational structure, some given problems from NoSQL solutions are easier to solve, such as aggregated query execution in an admissible time.

## ToroDB Stampede limitations

Not everything could be perfect and there are some known limitations from ToroDB Stampede.

* The only current MongoDB version supported is 3.2.
* [Capped collections](https://docs.mongodb.com/manual/core/capped-collections/) usage is not supported.
* If character `\0` is used in a string it will be escaped because PostgreSQL doesn't support it.
* Command `applyOps` reception will stop the replication server.
* Command `collMod` reception will be ignored.

In addition to the previous limitations, just some kind of indexes are supported:

* Index of type ascending and descending (those that ends in 1 and -1 when declared in mongo)
* Simple indexes of one key
* All keys path with the exception to the paths resolving in scalar value (eg: `db.test.createIndex({"a": 1})` will not index value of key `a` for the document `{"a": [1,2,3]}`)
* Index properties `sparse` and `background` are ignored

## When ToroDB Stampede might not be the right choice

As good as Stampede is, there are certain use-cases for which it is a bad choice or simply will not work:

* Pattern "key as values". When keys contain values, potentially thousands of different values may appear in keys, leading to an equally high number of columns 
(which might break with some RDBMS which have limits to the number of columns per row, see next point) and/or tables, which might be terribly inconvenient and slow.
* Too many fields per document, several of them optional and only some appearing per document, which might lead to thousands of columns. 
Some RDBMSs do not support such a high number of columns. For PostgreSQL this limit is around 1600 columns.


[TODO]: <> (not supported types, we need a list)

[Versions]: <> (this section doesn't make any sense currently)

[Documentation conventions]: <> (we have no time right now for this section)
