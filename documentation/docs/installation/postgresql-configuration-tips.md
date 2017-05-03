<h1>PostgreSQL configuration tips</h1>

The default parameters of PostgreSQL are usually very conservative about the underlying server capacity, so the configuration values are not very optimal.

The next sections suggests some better values, but there is nothing perfect for all cases and depends a lot on the the server setup and configuration.

## PostgreSQL configuration

At the `postgresql.conf` file some values can be adjusted.

### max_connections

Contrary to the general believe, a large number of connections is not so good. This number should be under 200 and if needed one connection pool like PgBouncer being used.

### shared_buffers

There are corner cases but in general for dedicated servers with 64 bits Linux systems with more than 1GB of memory, 1/4 of the total memory is recommended.

### work_mem

This is the memory used by internal sort operations and sometimes it is configured to higher values, actually it should be between 2MB and 4MB. For example, if there are 30 concurrent users querying and the value is set to 50MB the total memory used will be 1.5 GB.

If needed this value can be set from the client when the query is launched, using the command `SET work_mem = '32MB'`.

### maintenance_work_mem

It's safe to set this value significantly larger than work_mem.

### checkpoint_timeout

It is the maximum time between automatic WAL checkpoints. A value between 15 and 30 minutes is recommended.

### effective_cache_size

Setting the value to 1/2 of total memory would be a normal conservative setting, and 3/4 of memory is a more aggressive but still reasonable amount.

<!--
## Linux configuration

If the underlying OS is Linux some aspects can be optimized to get better a better performance.

### File system

TBD
-->