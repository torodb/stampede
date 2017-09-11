<h1>PostgreSQL configuration tips</h1>

The default parameters of PostgreSQL are usually very conservative about the underlying server capacity and might be suboptimal for moder hardware.

We cannot provide the best settings for all possible situation here, yet we provide some general guidelines in the following sections.

The mentioned setting names refer to the `postgresql.conf` file.

## max_connections

Contrary to the general believe, a large number of connections is not so good. This number should be under 200. If needed a connection pool like [PgBouncer](https://pgbouncer.github.io/) can be used to reduce the number of connection to PostgreSQL.

## shared_buffers

In general, a dedicated PostgreSQL server on 64 bit Linux and more than 1GB of memory, should use about 1/4 of the total memory.

## effective_cache_size

Setting the value to 1/2 of total memory would be a normal conservative setting, and 3/4 of memory is a more aggressive but still reasonable amount.

However, if you are running any other application on the same machine—such as the ToroDB JVM—, make sure you reduce the `effective_cache_size` by the amount of memory needed by other applications.


## work_mem

This is the memory used by internal operations (sorting, everything that uses hash tables). If you are using ToroDB Stampede for OLAP queries, the optimal `work_mem` setting will higly depend on the queries you are running.

Note that this setting is applicable per execution plan node: a single query can have multiple sort or hash-table operations. Likewise, queries running in parallel will also need the memory at the same time.

If needed this value can be set from the client when the query is launched, using the command `SET work_mem = '32MB'`.

## maintenance_work_mem

It's safe to set this value significantly larger than work_mem.

## checkpoint_timeout

It is the maximum time between automatic WAL checkpoints. A value between 15 and 30 minutes is recommended.

## synchronous_commit

Consider setting [`synchronous_commit`](https://www.postgresql.org/docs/9.6/static/runtime-config-wal.html#GUC-SYNCHRONOUS-COMMIT) to off if you can tolerate potential data loss—e.g., if you use ToroDB Stampede as replication target but keep the required redundancy in MongoDB.

This is similar to MongoDB's behavior: writes are acknlowledged back to the application before the non-volatile storage has confirmed the write operation. Even though those write operations were confirmed to the application, they can be lost if the server crashes shortly after. You can tune the [`wal_writer_delay`](https://www.postgresql.org/docs/9.6/static/runtime-config-wal.html#GUC-WAL-WRITER-DELAY) setting to mitigate the risk of potential data loss.

<!--
## Linux configuration

If the underlying OS is Linux some aspects can be optimized to get better a better performance.

### File system

TBD
-->
