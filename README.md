# ToroDB

[![Master branch build status](https://travis-ci.org/torodb/torodb.svg?branch=master)](https://travis-ci.org/torodb/torodb) [![Quality Gate](https://sonarqube.com/api/badges/gate?key=com.torodb:torodb-pom)](https://sonarqube.com/dashboard/index/com.torodb:torodb-pom)

ToroDB is a technology designed to fulfill the gap between document oriented
and SQL databases. There are two products that use this technology: ToroDB
Server and ToroDB Stampede. Both platforms are open source and any feedback,
contributions, help and/or patches are very welcome. Please join the
[torodb-dev][2] mailing list for further discussion.

For more information, please see [ToroDB's website][1]

## ToroDB Server
It is a MongoDB-compatible server that supports speaks the MongoDB Wire 
Protocol (and therefore can be used with the same drivers used to connect to 
any standard MongoDB server) but stores your data into a reliable and trusted 
ACID database. 

More information about ToroDB Server can be found on [its own folder](/server)
in this repository.

## ToroDB Stampede
ToroDB Stampede is a business analytic solution that replicates your data in 
real time from a MongoDB replica set into a SQL database, allowing you to use
any business intelligence products (like [Tableau][3] or [Pentaho][4]) to 
analyze NoSQL data.

More information about ToroDB Stampede can be found on 
[its own folder](/stampede) in this repository.

## Code QA
 * Master branch build status: [![Master branch build status](https://travis-ci.org/torodb/torodb.svg?branch=master)](https://travis-ci.org/torodb/torodb) [![Quality Gate](https://sonarqube.com/api/badges/gate?key=com.torodb:torodb-pom)](https://sonarqube.com/dashboard/index/com.torodb:torodb-pom)
 * Devel branch build status :  [![Devel branch build status](https://travis-ci.org/torodb/torodb.svg?branch=devel)](https://travis-ci.org/torodb/torodb) [![Quality Gate](https://sonarqube.com/api/badges/gate?key=com.torodb:torodb-pom:devel)](https://sonarqube.com/dashboard/index/com.torodb:torodb-pom:devel)

## Are you a developer? Want to contribute? Questions about the source code?

Please see [CONTRIBUTING][5].

[1]: http://www.torodb.com
[2]: https://groups.google.com/forum/#!forum/torodb-dev
[3]: http://www.tableau.com
[4]: http://www.pentaho.com/
[5]: https://github.com/torodb/torodb/blob/master/CONTRIBUTING.md
