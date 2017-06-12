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
* Use a `toropassFile` as as described in [Creating a `.toropass` file](/installation/prerequisites/#creating-a-toropass-file)

## Backend Connection Pooling

ToroDB Stampede uses a connection pool for the backend connections. Its size and timeout (if no connection is available) can be adjusted. The default configuration is as follows:

```json
backend:
  pool:
    connectionPoolTimeout: 10000
    connectionPoolSize: 30
```

The [Options Reference](options-reference.md#torodb-stampede-pool-configuration) explains these settings in detail.
