<h1>Configuring the MongoDB Connection</h1>

ToroDB Stampede will connect to MongoDB using no authentication and no SSL connection by default. You can set up the connection to MongoDB using `auth` and `ssl` sections in ToroDB Stampede configuration.

For example to connect using cr or scram_sha1 authentication mode with simple SSL support you may use following configuration:

```json
replication:
  replSetName: rs1
  syncSource: localhost:27017
  auth:
    mode: negotiate
    user: mymongouser
    source: mymongosource
  ssl:
    enabled: true
    allowInvalidHostnames: false
    caFile: mycafile.pem
```
