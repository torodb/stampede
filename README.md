# ToroDB

ToroDB is an open source project that turns your RDBMS into a
MongoDB-compatible server, supporting the MongoDB query API and
MongoDB's replication, but storing your data into a reliable and trusted
ACID database. ToroDB currently supports PostgreSQL as a backend, but
others will be added in the future.

ToroDB natively implements the MongoDB protocol, so you can use it with
MongoDB tools and drivers, and features a document-to-relational mapping
algorithm that transforms the JSON documents into relational tables.
ToroDB also offers a native SQL layer and automatic data normalization
and partitioning based on JSON documents' implicit schema.

ToroDB follows a RERO (Release Early, Release Often) policy. Current version is
considered a "developer preview" and hence is not suitable for
production use. Use at your own risk. However, any feedback,
contributions, help and/or patches are very welcome. Please join the
[torodb-dev][8] mailing list for further discussion.

For more information, please see [ToroDB's website][1], this
[latest presentation][7] or this [video recording of a presentation][11] about
ToroDB.


## Code QA
 * Master branch build status: [![Master branch build status](https://travis-ci.org/torodb/torodb.svg?branch=master)](https://travis-ci.org/torodb/torodb)
 * Devel branch build status :  [![Devel branch build status](https://travis-ci.org/torodb/torodb.svg?branch=devel)](https://travis-ci.org/torodb/torodb)


## Requisites

ToroDB is written in Java and requires:

* A suitable JRE, version 7 or higher. It has been mainly tested with Oracle JRE 8.
* A [PostgreSQL][2] database, version 9.4 or higher. [Download][9].


## Download/Installation

### Download the compiled file

You may download the latest version (v. 0.40) of ToroDB from
[the release page](https://github.com/torodb/torodb/releases/latest) on the
following packaging formats:
 * [tar.bz2](https://github.com/torodb/torodb/releases/download/v0.40/torodb-0.40-release.tar.bz2)
 * [zip](https://github.com/torodb/torodb/releases/download/v0.40/torodb-0.40-release.zip)

See below for instructions on how to run it.

You can also find binary files on [ToroDB's maven repository][3].


### Compile and install from sources

To get the latest version, you may compile ToroDB yourself. All the project is written in Java and managed with Maven, so you need a javac and maven.

ToroDB is based on the [Mongo Wire Protocol library][5] (mongowp), which is another library built by [8Kdata][6] to help construct programs that speak the MongoDB protocol. You may also compile this library yourself, or let maven download it from the repository automatically.

Just run `mvn package -Passembler` on the root directory and execute it from
`torodb/target/appassembler/bin` or choose your prefered packaging format from
`torodb/target/dist/`.


## Running ToroDB

ToroDB needs either a configuration file or some command-line parameters
to run. But it can also run without any of them if you follow some
conventions.

Before running ToroDB it is necessary to configure the RDBMS with the
ToroDB user that will be responsible to create namespaces, required data
types, tables and indexes.

Create user torodb (this is default user name, see ToroDB configuration
to use a different name):

    =# CREATE USER torodb WITH  SUPERUSER PASSWORD '<your-password>';

Create the database torod (this is default database name, see ToroDB
configuration to use a different name):

    =# CREATE DATABASE torod OWNER torodb;

The script $TOROHOME/bin/torodb (or torodb.bat) will run ToroDB. ToroDB can be
configured by a configuration file written in YAML or XML formats by
passing arguments -c or -x, respectively, to the script
$TOROHOME/bin/torodb. For example, to run ToroDB with configuration file
torodb.yml, run:

    $ $TOROHOME/bin/torodb -c torodb.yml

To print default configuration script in YAML or XML format use the
arguments -l an -lx respectively. For example to generate default YAML
configuration file:

    $ $TOROHOME/bin/torodb -l > torodb.yml

ToroDB connects to the backend database using user torodb (that has been
created in previous step). By default ToroDB reads the file
$HOME/.toropass (file path can be configured in the configuration), if
it exists, that stores the password in PostgreSQL's [.pgpass][4] syntax. The
password can also be specified in clear text in the configuration file or
will be asked at the prompt if the argument -W is issued.

To get general help, pass --help argument:

    $ $TOROHOME/bin/torodb --help

Use --help-param to get help on all available parameters of the
configuration file:

    $ $TOROHOME/bin/torodb --help-param

If you setup a .toropass, use torodb as the user and torod as the
database, ToroDB will run without a configuration file (with the rest of
the configuration values with their respective defaults).

Once ToroDB is running, connect to it with a normal MongoDB client,
like:

    $ mongo localhost:27018/torod

## Running ToroDB with Docker

A docker-compose.yml and Dockerfiles are provided to launch ToroDB without
too much hassle.

Docker & Docker compose must be installed and running.

Build the images:

    $ docker-compose build

Prepare & drink a coffee (or two)

Run ToroDB:

    $ docker-compose up

Connect to it with a normal MongoDB client, like:

    $ mongo localhost:27018/torod

Enjoy!

## Are you a developer? Want to contribute? Questions about the source code?

Please see [CONTRIBUTING][10].


[1]: http://www.torodb.com
[2]: http://www.postgresql.org
[3]: https://oss.sonatype.org/content/groups/public/com/torodb/torodb/
[4]: http://www.postgresql.org/docs/9.4/static/libpq-pgpass.html
[5]: https://github.com/8kdata/mongowp
[6]: http://www.8kdata.com
[7]: http://www.slideshare.net/8kdata/torodb-internals-how-to-create-a-nosql-database-on-top-of-sql-55275036
[8]: https://groups.google.com/forum/#!forum/torodb-dev
[9]: http://www.postgresql.org/download/
[10]: https://github.com/torodb/torodb/blob/master/CONTRIBUTING.md
[11]: https://www.youtube.com/watch?v=C2XuOhLrblo
