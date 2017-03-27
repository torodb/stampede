<h1>Configuring the PostgreSQL Connection</h1>

By default ToroDB Stampede connects to PostgreSQL using the following configuration:

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

You may change this configuration depending on your requisites.
You can enabled SSL connection setting `ssl: true` in configuration file.
To provide the PostgreSQL user's password that ToroDB Stampede will use to connect to PostgreSQL 
you can specify parameter `--ask-for-password` to make ToroDB Stampede prompt for the password while starting up  
or you create a PostgreSQL credentials configuration file `~/.toropass`, using the `.pgpass` file format. 
The right format is one or more lines formatted as `<host>:<port>:<database>:<user>:<password>`.

```no-highlight
echo "localhost:5432:torod:torodb:torodb" > ~/.toropass
chmod 400 ~/.toropass
```

You may change the `.toropass` path using the `toropassFile` parameter in ToroDB Stampede configuration file. For example:

```json
backend:
  postgres:
    host: localhost
    port: 5432
    database: torod
    user: torodb
    toropassFile: /secret/mytoropass
    applicationName: "toro"
    ssl: false
```
## Backend connection pooling

By default ToroDB Stampede uses a connection pool with the following configuration:

```json
backend:
  pool:
    connectionPoolTimeout: 10000
    connectionPoolSize: 30
```

You may tune those parameters at will. The only constraint is that `connectionPoolSize` has to be at least 20.
