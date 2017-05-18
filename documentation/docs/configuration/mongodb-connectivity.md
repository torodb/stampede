<h1>Configuring the MongoDB Connection</h1>

Per default, ToroDB Stampede uses no password and no SSL to connect to MongoDB. The `auth` and `SSL` settings can be used to change this behaviour:

The following examples uses cr or scram_sha1 authentication mode and a simple SSL setup:

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

## Replicate from a MongoDB Sharded Cluster


In the replication section of the yml config file add a shards item with the list of shards's configurations, one for each shard:

```json
replication:
  shards:
    - replSetName: shard1
      syncSource: localhost:27020
    - replSetName: shard2
      syncSource: localhost:27030
    - replSetName: shard3
      syncSource: localhost:27040
```
