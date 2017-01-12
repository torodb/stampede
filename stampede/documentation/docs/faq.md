<h1>Frequently Asked Questions</h1>

## Why that name?

Toro means bull in Spanish. ToroDB was founded in Madrid, Spain, by [8Kdata](https://8kdata.com/). It is the very first general-purpose database software ever built by a Spanish entity. We are very proud of this fact and wanted to name it after a well-known symbol of Spain, the toro. And the toro is a fast, solid, strong, but noble animal. Just like ToroDB.

## If ToroDB uses PostgreSQL, why not just base it on jsonb?

jsonb is a really cool data type for PostgreSQL, with a rich function set support that allows JSON data in a regular column, and it supports advanced indexing. jsonb was intended to allow adding some unstructured column(s) to your relational tables, and it fits really well for that purpose. But ToroDB's design and goals go way beyond jsonb's:

* Transform your unstructured data to a relational design, that leads to significant improvements in storage/IO/cache, having data partitioned by "type" and automatic data normalization.

* Provide native support for a NoSQL API --like ToroDB does with MongoDB's wire protocol and query API-- so you could directly use your MongoDB drivers, code and tools to interface with the database.

* Offer replication and sharding the same way NoSQL does (like replicating from a MongoDB replica set).

* Support non-PostgreSQL backends. While we love PostgreSQL, one size does not fit all, and other people have different requirements or different environments, like MPP (Massively Parallel) databases, in-memory solutions or just different stacks.

Still, ToroDB uses a little bit of jsonb internally: to represent arrays of scalar values; and to represent the structure table, which stores the "shape" ("type") of the documents in the collection.

## Which indexes are created?

ToroDB Stampede doesn't support all index types. Some indexes are supported or partialy supported, and other are skipped.

  * **Single field indexes**: Are fully supported.
  * **Compound indexes**: Are not supported and are not created.
  * **Multikey indexes**: The only multikey indexes created in ToroDB Stampede are those whose field(s) are in a embedded document. Multikey indexes over scalar values of an array are not created.
  * **Text indexes**: Are not supported and are not created.
  * **2dsphere indexes**: Are not supported and are not created.
  * **2d indexes**: Are not supported and are not created.
  * **Hashed indexes**: Are not supported and are not created.

Any created index can be explicitly [excluded in the configuration](installation/configuration.md#exclude-a-mongodb-index)     


## The command wget is not found in macOS

By default macOS hasn't the wget tool in the terminal, if you want to use it [Homebrew](http://brew.sh) can be used.

Once installed Homebrew, it can be installed with `brew install wget`.

## No pg_hba.conf entry

Depending on the running Linux distribution and PostgreSQL installation, the error below could appear.

```
FATAL: no pg_hba.conf entry for host "...", user "...", database "...", SSL off
```

This happens because some installations of PostgreSQL are configured with strict security policies. So PostgreSQL reject host connections through TCP. The `pg_hba.conf` file (usually located in the PostgreSQL's data directory or configuration directory) must be edited with a rule that allows access to the database for the ToroDB Stampede user.

```
  host    torod   torodb      127.0.0.1/32    md5
  host    torod   torodb      ::1/128         md5
```

__Make sure that new rules precede any other rule for same host that apply to all users (eg: 127.0.0.1/32). For more informations on `pg_hba.conf` refer to the [Official PostgreSQL documentation](https://www.postgresql.org/docs/current/static/auth-pg-hba-conf.html)__.
