<h1>Frequently Asked Questions</h1>

## Why that name?

Toro means bull in Spanish. ToroDB was founded in Madrid, Spain, by [8Kdata](https://8kdata.com/). It is the very first general-purpose database software ever built by a Spanish entity. We are very proud of this fact and wanted to name it after a well-known symbol of Spain, the toro. And the toro is a fast, solid, strong, but noble animal. Just like ToroDB.

## If ToroDB uses PostgreSQL, why not just base it on jsonb?

jsonb is a really cool PostgreSQL data type to store JSON documents in a column. It supports reach feature set that includes advanced indexing. jsonb is intended to better support unstructured column(s) in relational tables. But ToroDB's design and goals go way beyond jsonb's:

* Transform your unstructured data to a relational design, that leads to significant improvements in storage/IO/cache, having data partitioned by "type" and automatic data normalization.

* Provide native support for a NoSQL API—like ToroDB does with MongoDB's wire protocol and query API—so you could directly use your MongoDB drivers, code and tools to interface with the database.

* Offer replication and sharding the same way NoSQL does (like replicating from a MongoDB replica set).

* Support non-PostgreSQL backends. While we love PostgreSQL, one size does not fit all, and other people have different requirements or different environments, like MPP (Massively Parallel) databases, in-memory solutions or just different stacks.

Still, ToroDB uses a little bit of jsonb internally: to represent arrays of scalar values; and to represent the structure table, which stores the "shape" ("type") of the documents in the collection.

## What about ToroDB's performance?

Contrary to some popular beliefs, RDBMSs are not slow. Indeed, they can be quite fast. It's not hard, for instance, to achieve dozens or [hundreds of thousands of tps on RDBMSs like PostgreSQL](http://obartunov.livejournal.com/181981.html). The main problem is that benchmarks usually compare apples to oranges. Durability, for instance, is frequently reduced or suppressed in most NoSQL benchmarks, while it significantly impacts performance. The same goes on with replication. Take for instance a typical MongoDB benchmark, add journaling and replication (which you will very likely have turned on in a production environment), and your numbers will drop by an order of magnitude (160K tps vs 32K tps, 50% reads + 50% writes: [http://obartunov.livejournal.com/181981.html](http://obartunov.livejournal.com/181981.html)).

## What databases does ToroDB support as backends? Are there any plans to support other backends?

Currently, ToroDB supports PostgreSQL and MySQL as a backends. However, design and code have always kept in mind the possibility of supporting other backends. So it's technically possible and it will happen. Stay tuned!

## How do I optimally configure PostgreSQL for ToroDB?

As per ToroDB, there are no special configuration parameters required. So it really depends on your hardware characteristics, workload, network architecture and so on. Usual PostgreSQL configuration recommendations apply. There are hundreds of places on the Internet that discuss how to do this. You may start from [Tuning Your PostgreSQL Server](https://wiki.postgresql.org/wiki/Tuning_Your_PostgreSQL_Server) if you need some help.

Here are some recommendations though:

As with any other Postgres configuration, don't forget to tune the "ususal suspects" such as shared_buffers and checkpoint_segments (or max_wal_size if on 9.5).

Be aware of the memory allocated for PostgreSQL and the JVM if they are both co-located. If this is the case, you may probably want to allocate shared_buffers as you usually do, but reduce effective_cache_size by at least the maximum amount of heap allocated by the JVM (-Xmx).

Consider [setting synchronous_commit](http://www.postgresql.org/docs/9.4/static/runtime-config-wal.html) to off if you can tolerate some potential data loss. This will not corrupt your data in any way, and may improve performance. It is similar to MongoDB's behavior, where you may get writes acknowledged that may be lost if the server crashes during a small time window after the write happened. Please review wal_writer_delay if setting synchronous_commit to off to control the risk of potential data loss.

Make sure that ToroDB's configuration parameters generic.connectionPoolSize and generic.reservedReadPoolSize do not add up to more than max_connections.

Use data checksums for your PostgreSQL cluster if you want checksum validation at rest.

## What is ToroDB's license?

ToroDB is licensed under the GNU Affero General Public License v3 ([AGPLv3](https://www.gnu.org/licenses/agpl-3.0.html)). This means that ToroDB is free software, and you may freely use it, run it, modify and inspect it, as long as you comply with the terms of the license. As a non authoritative summary, this basically means that:

ToroDB is provided free of charge. Just download and use it.
If you make a derived version of ToroDB, or integrate ToroDB with other software, all of it must also be licensed under the AGPLv3 or a compatible license. This implies that users of your software will also have the same rights as ToroDB users, including access to ToroDB's source code. Copyright must also be preserved.

If you offer ToroDB or a derived work as a hosted service (like a DbaaS --Database as a Service--), your users are also bound by this license and the rights granted by the license also apply to them.

If you want to create a derived work or integrate ToroDB or parts of it into proprietary software, or do not want to be bound by the terms of the AGPLv3, please contact us at torodb at torodb dot com.

## What is MongoWP and how is it related to ToroDB?

MongoWP (Mongo Wire Protocol) is a component layer of ToroDB. However, it is being developed independently of ToroDB, and it is available at a [separate Github repository](https://github.com/8kdata/mongowp). MongoWP provides an abstraction layer for any Java-based software that would want to behave as a MongoDB server. It implements the MongoDB wire protocol and abstracts mongowp users from it. Just implement mongowp's API and start coding your own MongoDB server! It may also be the basis for other MongoDB-protocol related software such as clients (there's some basic client support in mongowp), proxies, query routers, etc.

MongoWP is based on Netty, a great asynchronous network I/O framework for the JVM. Netty is based on the event-based architecture, which does allocate a small number of threads for incoming connections, rather than a thread-per-connection, resulting in a really fast request dispatcher.

## What other open source components does ToroDB use?

* [PostgreSQL](http://www.postgresql.org/). The most advanced open source database.

* [MySQL](http://www.mysql.com/). The most popular open source database.

* [Netty](http://netty.io/), used by MongoWP. The great asynchronous network I/O framework for the JVM.

* [jOOQ](http://www.jooq.org/). jOOQ generates Java code from your database and lets you build type safe SQL queries through its fluent API.

* [HikariCP](http://brettwooldridge.github.io/HikariCP/). The fastest Java connection pooler.

* [Chronicle Queue](http://chronicle.software/products/chronicle-queue/). A distributed unbounded persisted queue. 

There are also many other Java libraries used by ToroDB like [ThreeTen](http://www.threeten.org/), [Guava](https://github.com/google/guava), [Guice](https://github.com/google/guice), [Findbugs](http://findbugs.sourceforge.net/), [jCommander](http://jcommander.org/), [Jackson](http://wiki.fasterxml.com/JacksonHome) and some others. We also use [Travis](https://travis-ci.org/) for CI tests.

ToroDB has the deepest gratitude to all the above projects, that are great components, and every other bit of open source that directly or indirectly helps building or running ToroDB.

## Which indexes are created?

ToroDB Stampede doesn't support all index types. Some indexes are supported or partialy supported, and other are skipped.

  * **Single field indexes**: Are fully supported.
  * **Compound indexes**: Are not supported and are not created.
  * **Multikey indexes**: The only multikey indexes created in ToroDB Stampede are those whose field(s) are in a embedded document. Multikey indexes over scalar values of an array are not created.
  * **Text indexes**: Are not supported and are not created.
  * **2dsphere indexes**: Are not supported and are not created.
  * **2d indexes**: Are not supported and are not created.
  * **Hashed indexes**: Are not supported and are not created.

Any created index can be explicitly [excluded in the configuration](/configuration/filtered-replication/#exclude-ignore-a-database-collections-or-indexes)
