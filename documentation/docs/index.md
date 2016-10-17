# Documentation

* [English version](en/00_Getting_started.md)
* [Spanish version](es/00_Getting_started.md)

# What is Toro Stampede?

In a MongoDB replica set, Toro Stampede can work as a node replicating the entire documental database in a relational way using PostgreSQL as a backend.

![Toro Stampede Structure](images/toro_stampede_structure.jpg)

This doesn't mean that JSON is stored using JSON support from PostgreSQL, it is actually stored with relational format. Each level from the JSON document is mapped to a table.

![Mapping example](images/toro_stampede_mapping.jpg)

With this approximation it is possible to have a NoSQL storage with actual relational capabilities without any change.
