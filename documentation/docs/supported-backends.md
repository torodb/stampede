<h1>Supported backends</h1>

As commented before, ToroDB Stampede requires the existence of a relational database to store the replicated data, this is called the backend.

Currently ToroDB Stampede just support PostgreSQL as a relational backend, but we are working in offer support for other options in the future.

Unlike MongoDB, most of the relational databases are not quite well tuned out of the box. That is why some tuning is recommended before using ToroDB Stampede in production, if not done before.

## PostgreSQL Tuning

PostgreSQL is one of those examples of relational database that are quite good but quite conservative out of the box.

A better understanding of PostgreSQL tuning process is recommended and the best way is reading the [official wiki](https://wiki.postgresql.org/wiki/Performance_Optimization). Below some basic parameters that can improve system behavior are explained.

### max_connections

Contrary to the general believe, a large number of connections is not so good. This number should be under 200 and if needed one connection pool like PgBouncer being used.

### shared_buffers

There are corner cases but in general for dedicated servers with 64 bits Linux systems with more than 1GB of memory, 1/4 of the total memory is recommended.

### work_mem

This is the memory used by internal sort operations and sometimes it is configured to higher values, actually it should be between 2MB and 4MB. For example, if there are 30 concurrent users querying and the value is set to 50MB the total memory used will be 1.5 GB.

If needed this value can be set from the client when the query is launched, using the command `SET work_mem = '32MB'`.
