<h1>Frequently Asked Questions</h1>

## Why that name?

Toro means bull in Spanish. ToroDB was founded in Madrid, Spain, by [8Kdata](https://8kdata.com/). It is the very first general-purpose database software ever built by a Spanish entity. We are very proud of this fact and wanted to name it after a well-known symbol of Spain, the Toro. And the Toro is a fast, solid, strong, but noble animal. Just like ToroDB.

## If ToroDB uses PostgreSQL, why not just base it on jsonb?

PostgreSQL's `jsonb` is a very powerful data type to support JSON data in a regular column. Its main use case is to add unstructured column(s) to a relational model. But ToroDB's design and goals go way beyond `jsonb`:

* Transform your unstructured data to a [relational schema](relational-schema.md).  
This leads to significant improvements in storage, IO, and caching because it automatically partitions data by "type".

* Provide native support for a NoSQL API.  
ToroDB supports MongoDBs wire protocol and query API so you can your MongoDB drivers, code and tools to access the data in PostgreSQL.

* Offer replication and sharding the same way NoSQL does.  
ToroDB can stream MongoDB's replica set oplog and thus become a replication target. This enables you to continue using MongoDB for your operative systems but at the same time use PostgreSQL rich SQL for analytics.

* Support non-PostgreSQL backends.  
We love PostgreSQL. Nevertheless you might prefer the SQL database you already have or need a specialized solution like MPP (Massively Parallel) or in-memory databases.

Still, ToroDB uses a little bit of `jsonb` internally: to represent arrays of scalar values; and to represent the structure table, which stores the "shape" ("type") of the documents in the collection.

## What databases does ToroDB support as backend?

Currently, ToroDB only supports PostgreSQL as a backend.

## Are there any plans to support other backends?

It was always a design principle to support other backends too. It is technically possible and is on your roadmap. Stay tuned!

## What about ToroDB's performance?

Contrary to some popular beliefs, RDBMSs are not slow. Indeed, they can be quite fast. It's not hard, for instance, to achieve dozens or [hundreds of thousands of tps on RDBMSs like PostgreSQL](http://obartunov.livejournal.com/181981.html). The main problem is that benchmarks usually compare apples to oranges.

Durability, for instance, is often reduced or even disabled in NoSQL benchmarks--this has a significant impact on performance. If you take a typical MongoDB benchmark, change the configuration to reflect a typical production environment (e.g., add journaling and replication) the numbers drop by an order of magnitude (160K tps vs 32K tps, 50% reads + 50% writes: [http://obartunov.livejournal.com/181981.html](http://obartunov.livejournal.com/181981.html)).

## How do I optimally configure PostgreSQL for ToroDB?

ToroDB doesn't have any special requirements. The PostgreSQL configuration should match your hardware and workload.

Nevertheless, we have some [PostgreSQL configuration tips](configuration/postgresql-configuration-tips.md). Beyond that, any general PostgreSQL tuning guide applies--e.g., [Tuning Your PostgreSQL Server](https://wiki.postgresql.org/wiki/Tuning_Your_PostgreSQL_Server).

## What is ToroDB's license?

ToroDB is licensed under the GNU Affero General Public License v3 ([AGPLv3](https://www.gnu.org/licenses/agpl-3.0.html)). This means that ToroDB is free software, and you may freely use it, run it, modify and inspect it, as long as you comply with the terms of the license. As a non authoritative summary, this basically means that:

ToroDB is provided free of charge. Just download and use it.
If you make a derived version of ToroDB, or integrate ToroDB with other software, all of it must also be licensed under the AGPLv3 or a compatible license. This implies that users of your software will also have the same rights as ToroDB users, including access to ToroDB's source code. Copyright must also be preserved.

If you offer ToroDB or a derived work as a hosted service (like a DbaaS --Database as a Service--), your users are also bound by this license and the rights granted by the license also apply to them.

If you want to create a derived work or integrate ToroDB or parts of it into proprietary software, or do not want to be bound by the terms of the AGPLv3, please contact us at torodb at torodb dot com.

## What is MongoWP and how is it related to ToroDB?

MongoWP (Mongo Wire Protocol) is a component layer of ToroDB. However, it is developed independently of ToroDB at a [separate GitHub repository](https://github.com/8kdata/mongowp).

MongoWP provides an abstraction layer for Java-based software to behave like a MongoDB server. It implements the MongoDB wire protocol and abstracts MongoWP users from it. Just implement MongoWP's API and start coding your own MongoDB server!

MongoWP has basic client support as well so that it might be useful for other MongoDB-protocol related software as well (clients, proxies, query routers, etc.)

MongoWP is based on [Netty](http://netty.io/), a great asynchronous network I/O framework for the JVM. Netty is based on the event-based architecture, which allocates a small number of threads for incoming connections, rather than a thread-per-connection, resulting in a really fast request dispatcher.

## What other open source components does ToroDB use?

* [PostgreSQL](http://www.postgresql.org/). The most advanced open source database.

* [Netty](http://netty.io/), used by MongoWP. The great asynchronous network I/O framework for the JVM.

* [jOOQ](http://www.jooq.org/). jOOQ generates Java code from your database and lets you build type safe SQL queries through its fluent API.

* [HikariCP](http://brettwooldridge.github.io/HikariCP/). The fastest Java connection pooler.

There are also many other Java libraries used by ToroDB like [ThreeTen](http://www.threeten.org/), [Guava](https://github.com/google/guava), [Guice](https://github.com/google/guice), [Findbugs](http://findbugs.sourceforge.net/), [jCommander](http://jcommander.org/), [Jackson](http://wiki.fasterxml.com/JacksonHome) and some others. We also use [Travis](https://travis-ci.org/) for CI tests.

ToroDB has the deepest gratitude to all the above projects, that are great components, and every other bit of open source that directly or indirectly helps building or running ToroDB.

## Which indexes are created?

ToroDB Stampede doesn't support all index types. Some indexes are supported or partially supported, and other are skipped.

  * **Single field indexes**: Are fully supported.
  * **Compound indexes**: Are not supported and are not created.
  * **Multikey indexes**: The only multikey indexes created in ToroDB Stampede are those whose field(s) are in an embedded document. Multikey indexes over scalar values of an array are not created.
  * **Text indexes**: Are not supported and are not created.
  * **2dsphere indexes**: Are not supported and are not created.
  * **2d indexes**: Are not supported and are not created.
  * **Hashed indexes**: Are not supported and are not created.

Any created index can be explicitly [excluded in the configuration](configuration/replication-exclusion.md)

[TODO]: <> (The link to proove "160K tps vs 32K tps, 50% reads + 50% writes: " seems to be the wrong one.)
[TODO]: <> (sentence "Use data checksums for your PostgreSQL cluster if you want checksum validation at rest." removed because I don't understand it)
