# ToroDB Stampede

> Transform your NoSQL data from a MongoDB replica set into a relational database in PostgreSQL.

There are other solutions that are able to store the JSON document in a 
relational table using PostgreSQL JSON support, but it doesn't solve the real 
problem of 'how to really use that data'. ToroDB Stampede replicates the 
document structure in different relational tables and stores the document data
in different tuples using those tables.

![](documentation/docs/images/tables_distribution.jpeg)

## Installation

Due to the use of different external systems like MongoDB and PostgreSQL, the
installation requires some previous steps. Take a look at out 
[quickstart][1] in the
documentation.

## Usage example

MongoDB is a great idea, but sooner or later some kind of business 
intelligence, or complex aggregated queries are required. At this point MongoDB
is not so powerful and ToroDB Stampede borns to solve that problem (see 
[our post about that][2]).

The kind of replication done by ToroDB Stampede allows the execution of 
aggregated queries in a relational backend (PostgreSQL) with a noticeable time 
improvement.

A deeper explanation is available in our 
[how to use][3] section in the 
documentation.

## Development setup

As it was said in the installation section, the requirements of external 
systems can make more difficult to explain briefly how to setup the development 
environment here. So if you want to take a look on how to prepare your 
development environment, take a look to our 
[documentation][4].

## Release History

* 1.0.0-beta2
    * Released on April 06th 2017
* 1.0.0-beta1
    * Released on December 30th 2016

## Meta

ToroDB – [@nosqlonsql](https://twitter.com/nosqlonsql) – info@8kdata.com

Distributed under the GNU AGPL v3 license. See ``LICENSE`` for more information.

[1]: https://www.torodb.com/stampede/docs/quickstart
[2]: https://www.8kdata.com/blog/the-conundrum-of-bi-aggregate-queries-on-mongodb/
[3]: https://www.torodb.com/stampede/docs/how-to-use
[4]: https://www.torodb.com/stampede/docs/installation/previous-requirements/
