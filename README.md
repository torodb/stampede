ToroDB
======

[ToroDB][1] is an open source, document-oriented, JSON database that runs on top of PostgreSQL. JSON documents are stored relationally, not as a blob/jsonb. This leads to significant storage and I/O savings. It speaks natively the MongoDB protocol, meaning that it can be used with any mongo-compatible client.

ToroDB follows a RERO (Release Early, Release Often) policy. Current version is considered a "developer preview" and hence is not suitable for production use. However, any feedback, contributions, help and/or patches are very welcome.


Requisites
----------

ToroDB is written in Java and requires:

* A suitable JVM, version 6 or higher. It has been tested with Oracle JVM v8.
* A [PostgreSQL][2] database, version 9.4. PostgreSQL 9.4 is currently in beta3.


Installation
------------

You may compile ToroDB yourself. All the project is written in Java and managed with Maven, so you need a javac and maven. Just execute "`mvn package`" on the root directory and find the executable jar file in `torodb/target/torodb-0.12-jar-with-dependencies.jar`.

Alternatively, you may download a [compiled version][3] from ToroDB's maven repository.


Running ToroDB
--------------

Execute with `java -jar <path>/torodb-0.12-jar-with-dependencies.jar <arguments>`. If you run with `--help`, you will see the required and optional arguments to run ToroDB:

    --ask-for-password
       Force input of PostgreSQL's database user password.
       Default: false
    -c, --connections
       Number of connections to establish to the PostgreSQL database
       Default: 10
    -d, --dbname
       PostgreSQL's database name to connect to (must exist)
       Default: torod
    -p, --dbport
       PostgreSQL's server port
       Default: 5432
    --debug
       Change log level to DEBUG
       Default: false
    -h, --host
       PostgreSQL's server host (hostname or IP address)
       Default: localhost
    -P, --mongoport
       Port to listen on for Mongo wire protocol connections
       Default: 27017
    --help, --usage
       Print this usage guide
       Default: false
    -u, --username
       PostgreSQL's database user name. Must be a superuser
       Default: postgres
    --verbose
       Change log level to INFO
       Default: false

The database must exist, and the username must have superuser privileges. This will be changed in the future.

Alternatively to the command line options, you may create a `~/.toropass` file, which follows the syntax of [PostgreSQL's pgpass][4] files.


[1]: http://www.torodb.com
[2]: http://www.postgresql.org
[3]: http://maven.torodb.com/release/com/torodb/torodb/0.12/torodb-0.12-jar-with-dependencies.jar
[4]: http://www.postgresql.org/docs/9.3/static/libpq-pgpass.html
