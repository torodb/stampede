<h1>Configuring Filtered Replication</h1>

By default ToroDB Stampede replicates all databases and collections available in your MongoDB. You can configure ToroDB Stampede to limit the replication by specifying which databases, collections, and indexes to include or exclude from replcation.

Note that exclusions always override inclusions—i.e. if you exclude something it will not be replicated even if you include the same thing.

!!! danger "Changing Include and Exclude Configuration"
    ToroDB Stampede does not keep track of changes to the **configuration**.

    If you stop ToroDB Stampede, remove a database or collection inclusion, and restart ToroDB Stampede, the replication process will replicate operations on this database/collection without replicating previously data form not included database/collection, reaching an inconsistent state. In such cases, it is recommended to delete ToroDB Stampede database and restart the whole replication process from scratch.

    The same is true for indexes: ToroDB Stampede only creates indexes at the initial recovery process and when a create index command is found in the oplog replication process, not because of configuration changes.

!!! note "Demo Setup"
    The following examples assume two databases (`films` and `music`) wheras each has two collections (`title` and `performer`).


## Include: Select a Database, its Collections and Indexes

### Databases

To limit the replication to a single database (`<database name>`), use the `include` setting in the `replication` section of the configuration file:

```json
replication:
  replSetName: rs1
  syncSource: localhost:27017
  include:
    <database name>: "*"
```

### Collections

To further limit the replication to selected collections of this database, list them below the database:

```json
replication:
  replSetName: rs1
  syncSource: localhost:27017
  include:
    <database name>:
      - <collection name 1>
      - <collection name 2>
```

### Indexes

Likewise, you can limit the indexes that are automatically created in the relational backend by ToroDB Stampede:

```json
replication:
  replSetName: rs1
  syncSource: localhost:27017
  include:
    <database name>:
      <collection name>:
        - name: <index name>
```

The following example limits the replication to the `performer` collection in the `film` database and only creates the `city` index in the SQL backend:

```json
replication:
  replSetName: rs1
  syncSource: localhost:27017
  include:
    film:
      performer:
        - name: "city"
```

## Exclude: Ignore a Database, Collections, or Indexes

### Databases

To exclude a database (`<database name>`) from replication, use the `exclude` setting in the `replication` section of the configuration file:

```json
replication:
  replSetName: rs1
  syncSource: localhost:27017
  exclude:
    <database name>: "*"
```

### Collections

If you want to exclude some collections (but still repliate the others), list them below the database:

```json
replication:
  replSetName: rs1
  syncSource: localhost:27017
  exclude:
    <database name>:
      - <collection name 1>
      - <collection name 2>
```

### Indexes

Some indees created in MongoDB for OLTP operations might be useless for OLAP and analytics operations in the SQL backend. You can easily exclude them by listing the indexes that should not be created in the SQL backend below the respective collection:

```json
replication:
  replSetName: rs1
  syncSource: localhost:27017
  exclude:
    <database name>:
      <collection name>:
        - name: <index name>
```

The following example only excludes a single index from the replication: namely, the index `city` on the collection `performer` in the `film` database:

```json
replication:
  replSetName: rs1
  syncSource: localhost:27017
  exclude:
    film:
      performer:
        - name: city
```

!!! note "Unsupported index types are always excluded"
    ToroDB Stampede generally ignores MongoDB indexes that are not yet supported (text, 2dsphere, 2d, hashed, ...).

## Mixing Include and Exclude

You can combine the `include` and `exclude` sections to limit the replication to a single database, but exclude a collections and or indexes.

The following example only replicates the `file` database, but excludes the collection `performer` from it.

```json
replication:
  replSetName: rs1
  syncSource: localhost:27017
  include:
    film: "*"
  exclude:
    film: "performer"
```

The next example limits the replication to the `performer` collection from the `film` database but excludes the index `city` from that collection:

```json
replication:
  replSetName: rs1
  syncSource: localhost:27017
  include:
    film:
      performer: "*"
  exclude:
    film:
      performer:
        - name: "city"
```
