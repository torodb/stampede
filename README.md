# ToroDB

ToroDB is an open source, document-oriented, JSON database that runs on top of PostgreSQL. JSON documents are stored relationally, not as a blob/jsonb. This leads to significant storage and I/O savings. It speaks natively the MongoDB protocol, meaning that it can be used with any mongo-compatible client.

ToroDB follows a RERO (Release Early, Release Often) policy. Current version is considered a "developer preview" and hence is not suitable for production use. However, any feedback, contributions, help and/or patches are very welcome. Please join the [torodb-dev][8] mailing list for further discussion.

For more information, please see [ToroDB's website][1], this [latest presentation][7] or this [video recording of a presentation][11] about ToroDB.


## Requisites

ToroDB is written in Java and requires:

* A suitable JRE, version 6 or higher. It has been mainly tested with Oracle JRE 8.
* A [PostgreSQL][2] database, version 9.4 or higher. [Download][9].


## Download/Installation

### Download the compiled jar file

You may download the [latest version][3] (v. 0.22.1) of ToroDB from ToroDB's maven repository. See below for instructions on how to run it.


### Compile and install from sources

You may compile ToroDB yourself. All the project is written in Java and managed with Maven, so you need a javac and maven.

ToroDB is based on the [Mongo Wire Protocol library][5] (mongowp), which is another library built by [8Kdata][6] to help construct programs that speak the MongoDB protocol. You may also compile this library yourself, or let maven download it from the repository automatically.

Just run `mvn package` on the root directory and find the executable jar file in `torodb/target/torodb-0.22.1-jar-with-dependencies.jar`.


## Running ToroDB

Execute with `java -jar <path>/torodb.jar <arguments>`. If you run with `--help`, you will see the required and optional arguments to run ToroDB:

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


## Are you a developer? Want to contribute? Questions about the source code?

Please see [CONTRIBUTING][10].



[1]: http://www.torodb.com
[2]: http://www.postgresql.org
[3]: http://maven.torodb.com/jar/com/torodb/torodb/0.22.1/torodb.jar
[4]: http://www.postgresql.org/docs/9.3/static/libpq-pgpass.html
[5]: https://github.com/8kdata/mongowp
[6]: http://www.8kdata.com
[7]: http://www.slideshare.net/8kdata/big-dataspain2014-torodbbridgebetweennosqlandrelational
[8]: https://groups.google.com/forum/#!forum/torodb-dev
[9]: http://www.postgresql.org/download/
[10]: https://github.com/torodb/torodb/blob/master/CONTRIBUTING.md
[11]: http://www.bigdataspain.org/2014/conference/new-open-source-database-a-bridge-between-the-nosql-and-relational-worlds
