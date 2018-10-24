## Release Notes for Stampede 1.0.0

### Changes

* Support MySQL as backend

### Bugs Fixed

* Many many bugfixes (see logs for [MongoWP](https://github.com/torodb/mongowp/commits/0.50.2), [ToroDB Engine](https://github.com/torodb/engine/commits/v0.50.3) and [ToroDB Stampede](https://github.com/torodb/stampede/commits/v1.0.0)

## Release Notes for Stampede 1.0.0-beta3

### Changes


### Bugs Fixed

* Stampede use wrong format for dates before year 1 using PostgreSQL's COPY.
* Stampede give an internal error when MongoDB password is not found and authentication is enabled.

## Release Notes for Stampede 1.0.0-beta2

### Changes

* Add support for MongoDB 3.4
    * Deal with MongoDB views
    * Support BSON Type Decimal128
* Support Sharding Replication. ToroDB Stampede can replicate from N shards into the same ToroDB database.
    *  Adjust Guice to provide a better way to inject different metrics and loggers
    *  Adapt metrics and logging so each shard has their own values
    *  Adapt the Data Import Mode to the sharding model
* Stampede Packaging
    * RPM package
    * DEB package
    * Snap package
* Allow SSL connection to the backend (PostgreSQL)
* Add FlexyPool to ToroDB
* Integration Tests
* Support all BSON types  
* Deal with system collections
* Unify logging system and improve error messages
* Calibrate maximum threads using also connection pool size
* Review and test Windows/Mac installation/configuration documentation
* Improve ToroDB Parameter configuration


### Bugs Fixed

* Stampede did not support documents whose '\_id' is a container (object or array)
