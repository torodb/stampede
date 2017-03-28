<h1>Configuring the PostgreSQL Connection</h1>

ToroDB Stampede uses the following defaults to connect to PostgreSQL:

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
The [Options Reference](options-reference.md#postgresql-configuration) explains these settings in detail.

## Password Configuration

There are two ways to provide the PostgreSQL password:

* Use the `--ask-for-password` option to enter the password manually at startup
* Use a `toropassFile` as follows:  

    1. Create a file in [`.pgpass` format](https://wiki.postgresql.org/wiki/Pgpass): `<host>:<port>:<database>:<user>:<password>`  
    `echo "localhost:5432:torod:torodb:torodb" > ~/.toropass`
    1. Make sure the file's permissions are restrictive  
    `chmod 400 ~/.toropass`
    1. If you don't want to use the default location (`~/.toropass`) move the file and configure the path in the YAML configuration setting `toropassFile`.

## Backend Connection Pooling

ToroDB Stampede uses a connection pool for the backend connections. Its size and timeout (if no connection is available) can be adjusted. The default configuration is as follows:

```json
backend:
  pool:
    connectionPoolTimeout: 10000
    connectionPoolSize: 30
```

The [Options Reference](options-reference.md#torodb-stampede-pool-configuration) explains these settings in detail.
