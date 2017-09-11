<h1>The Relational Schema</h1>

To explain the mapping from JSON documents to the relational database, we use using the [example dataset](https://docs.mongodb.com/getting-started/shell/import-data/) from the MongoDB documentation.

!!! note "Prerequisits to run the examples"
    You need a running ToroDB Stampede setup, including the properly configured replication from MongoDB to PostgreSQL.

Download and import the *primer* dataset into the `primer` collection of the `stampede` MongoDB database:

```no-highlight
wget https://www.torodb.com/download/primer-dataset.json

mongoimport -d stampede -c primer primer-dataset.json
```

ToroDB Stampede will replicate this collection to PostgreSQL into the schema `stampede` of the `torod` database (unless [configured otherwise](/configuration/postgresql-connectivity/)).

For each collection, ToroDB Stampede creates a *root table* with the collections name (`primer` in this case). Further, ToroDB Stampede creates several associated tables with names like `primer_*`.

## Example

Essentially, each level of the JSON document is mapped to a different table in the relational backend.

For demonstration, consider the following example document from the primer dataset:

```
{
  "address": {
     "building": "1007",
     "coord": [ -73.856077, 40.848447 ],
     "street": "Morris Park Ave",
     "zipcode": "10462"
  },
  "borough": "Bronx",
  "cuisine": "Bakery",
  "grades": [
     { "date": { "$date": 1393804800000 }, "grade": "A", "score": 2 },
     { "date": { "$date": 1378857600000 }, "grade": "A", "score": 6 },
     { "date": { "$date": 1358985600000 }, "grade": "A", "score": 10 },
     { "date": { "$date": 1322006400000 }, "grade": "A", "score": 9 },
     { "date": { "$date": 1299715200000 }, "grade": "B", "score": 14 }
  ],
  "name": "Morris Park Bake Shop",
  "restaurant_id": "30075445"
}
```

For this document, ToroDB Stampede creates four tables:

* `primer`  (the root table, named after the MongoDB collection)
* `primer_address`
* `primer_address_coord`
* `primer_grades`

![Tables distribution](images/tables_distribution.jpeg)

### The Tables Created

Each of the created tables has at least one meta column: `did` for document id. Further [meta columns](#metadata-columns) depend on the position and type of the data.

#### primer

Each element of the root level is mapped to a different column of the table, either a scalar or subdocument. The name of the column consists of the attribute name postfixed by a type identifier.

The `cuisine` is mapped as `cuisine_s` because it contains a string value. See [Column Type Encoding](#column-type-encoding) for a list of all postfixes.

```no-highlight
did  | address_e | restaurant_id_s |             name_s                |   cuisine_s   |           _id_x            |   borough_s   | grades_e
-----+-----------+-----------------+-----------------------------------+---------------+----------------------------+---------------+----------
   0 | f         | 40384115        | Phil & Sons Restaurant & Pizzeria | Pizza/Italian | \x580f12efbe6e3fff2237caef | Queens        | t
   1 | f         | 40384100        | Josie'S Restaurant                | American      | \x580f12efbe6e3fff2237caee | Manhattan     | t
   2 | f         | 40384036        | Mcdonald'S                        | Hamburgers    | \x580f12efbe6e3fff2237caed | Brooklyn      | t
   3 | f         | 40383945        | Pizza D'Oro                       | Pizza         | \x580f12efbe6e3fff2237caec | Staten Island | t
   4 | f         | 40383931        | Flash Dancers                     | American      | \x580f12efbe6e3fff2237caeb | Manhattan     | t
   5 | f         | 40383836        | Barone Pizza                      | Pizza         | \x580f12efbe6e3fff2237caea | Queens        | t
   6 | f         | 40383825        | Russian Samovar                   | Russian       | \x580f12efbe6e3fff2237cae9 | Manhattan     | t
   7 | f         | 40383819        | Short Stop Restaurant             | American      | \x580f12efbe6e3fff2237cae8 | Bronx         | t
```

#### primer_address

The `primer_address` table represents the nested `address` document. The reference to the document each address belongs to is given by the `did` meta column.

The `rid` meta column is a unique identifier of the row in each table (the primary key).

```no-highlight
did  |  rid  | seq | zipcode_s | coord_e |                street_s                | building_s
-----+-------+-----+-----------+---------+----------------------------------------+------------
   0 |     0 |     | 11355     | t       | Main Street                            | 57-29
   1 |     1 |     | 10023     | t       | Amsterdam Avenue                       | 300
   2 |     2 |     | 11207     | t       | Pennsylvania Avenue                    | 819
   3 |     3 |     | 10314     | t       | Victory Boulevard                      | 3115
   4 |     4 |     | 10019     | t       | Broadway                               | 1674
   5 |     5 |     | 11354     | t       | Main Street                            | 4027
   6 |     6 |     | 10019     | t       | West   52 Street                       | 256
   7 |     7 |     | 10463     | t       | Broadway                               | 5977
```

#### primer_address_coord

The `primer_address_coord` table represents the `coord` attribute of the `address`, which is an array. As arrays is an ordered collection, while relational tables are not ordered, ToroDB Stampede uses the meta column `seq` to indicate the position of the element in the original array.

Also note the meta column `pid`, which refers to the `rid` of the parent level (`primer_address` in this case).

```no-highlight
did  |  rid  |  pid  | seq |     v_d      
-----+-------+-------+-----+--------------
   0 |     0 |     0 |   0 |   -73.825679
   0 |     1 |     0 |   1 |   40.7455975
   1 |     2 |     1 |   0 |  -73.9809789
   1 |     3 |     1 |   1 |   40.7802374
   2 |     4 |     2 |   0 |  -73.8896643
   2 |     5 |     2 |   1 |   40.6578505
   3 |     6 |     3 |   0 |  -74.1630372
   3 |     7 |     3 |   1 |     40.60731
   4 |     8 |     4 |   0 |   -73.982872
   4 |     9 |     4 |   1 |   40.7628094
   5 |    10 |     5 |   0 |   -73.829714
   5 |    11 |     5 |   1 |   40.7587648
   6 |    12 |     6 |   0 |   -73.984752
   6 |    13 |     6 |   1 |    40.763105
   7 |    14 |     7 |   0 |  -73.8982704
   7 |    15 |     7 |   1 |   40.8896923
```

#### primer_grades

The `primer_grades` table represents an array and uses the `seq` meta column for this reason. As this element is a direct child of the root level, no `pid` is necessary.

Note that the `score` element was mapped to two columns: `score_i` (`integer`) and `score_n` to indicate null (as opposed to a missing key).

```no-highlight
did  |  rid  | seq |         date_t         | score_i |    grade_s     | score_n
-----+-------+-----+------------------------+---------+----------------+---------
   0 |     0 |   0 | 2014-08-21 02:00:00+02 |       6 | A              |
   0 |     1 |   1 | 2014-02-03 01:00:00+01 |      19 | B              |
   0 |     2 |   2 | 2013-04-13 02:00:00+02 |       7 | A              |
   0 |     3 |   3 | 2012-10-17 02:00:00+02 |       9 | A              |
   0 |     4 |   4 | 2011-10-22 02:00:00+02 |      10 | A              |
   1 |     5 |   0 | 2014-02-20 01:00:00+01 |      10 | A              |
   1 |     6 |   1 | 2013-07-22 02:00:00+02 |      12 | A              |
   1 |     7 |   2 | 2012-06-25 02:00:00+02 |      10 | A              |
   1 |     8 |   3 | 2011-11-16 01:00:00+01 |      22 | B              |
   1 |     9 |   4 | 2011-04-26 02:00:00+02 |      12 | A              |
   2 |    10 |   0 | 2014-04-24 02:00:00+02 |       3 | A              |
   2 |    11 |   1 | 2013-10-10 02:00:00+02 |       4 | A              |
   2 |    12 |   2 | 2013-05-08 02:00:00+02 |       2 | A              |
   2 |    13 |   3 | 2012-11-23 01:00:00+01 |       7 | A              |
   2 |    14 |   4 | 2012-03-05 01:00:00+01 |      19 | B              |
   2 |    15 |   5 | 2011-09-22 02:00:00+02 |      12 | A              |
   2 |    16 |   6 | 2011-08-16 02:00:00+02 |       3 | P              |
   3 |    17 |   0 | 2014-08-07 02:00:00+02 |      21 | B              |
   3 |    18 |   1 | 2014-01-07 01:00:00+01 |      13 | A              |
   3 |    19 |   2 | 2012-10-09 02:00:00+02 |      13 | A              |
   3 |    20 |   3 | 2011-10-18 02:00:00+02 |       4 | A              |
   4 |    21 |   0 | 2014-02-20 01:00:00+01 |       8 | A              |
   4 |    22 |   1 | 2013-01-25 01:00:00+01 |      13 | A              |
   4 |    23 |   2 | 2011-12-27 01:00:00+01 |      10 | A              |
   5 |    24 |   0 | 2014-11-13 01:00:00+01 |      16 | B              |
   5 |    25 |   1 | 2014-04-16 02:00:00+02 |       7 | A              |
   5 |    26 |   2 | 2013-10-10 02:00:00+02 |       5 | A              |
   5 |    27 |   3 | 2013-03-08 01:00:00+01 |       9 | A              |
   5 |    28 |   4 | 2012-08-22 02:00:00+02 |      44 | C              |
   6 |    29 |   0 | 2014-12-30 01:00:00+01 |      40 | Z              |
   6 |    30 |   1 | 2014-05-29 02:00:00+02 |      10 | A              |
   6 |    31 |   2 | 2013-09-24 02:00:00+02 |      10 | A              |
   6 |    32 |   3 | 2013-02-12 01:00:00+01 |      18 | B              |
   6 |    33 |   4 | 2012-05-11 02:00:00+02 |       6 | A              |
   7 |    34 |   0 | 2014-05-30 02:00:00+02 |       6 | A              |
   7 |    35 |   1 | 2013-04-25 02:00:00+02 |       7 | A              |
   7 |    36 |   2 | 2012-11-20 01:00:00+01 |      12 | A              |
   7 |    37 |   3 | 2012-05-30 02:00:00+02 |      10 | A              |
   7 |    38 |   4 | 2011-12-19 01:00:00+01 |      18 | B              |
```

## Metadata Columns

ToroDB Stampede creates different metadata columns in the data tables.

| Column | What does it? |
|--------|---------------|
| did | It is the unique identifier of the document and it has the same value for all rows related to the same document. |
| rid | It is the unique identifier of the row, for example when an array is mapped the rid is different for each row but the did is the same. |
| pid | It is the reference to the parent rid. For example, `primer_address_coord` has elements with rid 0 and 1 and pid 0, that means they are childs of row with rid 0 at `primer_address`. |
| seq | Represents the position of an element inside the original array. |

![PID reference](images/pid_reference.jpeg)


## Identifier Mappings

In the example above, ToroDB Stampede used the identifier names from MongoDB (collection and JSON field names) to build the identifier names in the SQL backend (table and column names). Although this approach works in many cases, it is not always possible to map identifier names in this way.

One of the possible problems that might occur is that deeply nested JSON documents lead to SQL identifier names that exceed the maximum identifier length of the SQL backend (in PostgreSQL, 63 bytes).

To avoid this problem, ToroDB Stampede doesn't rely on a syntactic transformation of identifier names, but uses mapping tables in the `torodb` schema. All of them basically map the names from an MongoDB element (database, collection, ...) to an SQL identifier (which happens to be called `identifier` in all these tables).

### database

Table `database` maps MongoDB database names (column `name`) to database names used in PostgreSQL (column `identifier`).

```no-highlight
# select * from database;

   name   | identifier
----------+------------
 stampede | stamped
```

### collection

Each replicated collection name (column `name`) within a MongoDB database (column `database`) is mapped to a PostgreSQL name (column `identifier`).

```no-highlight
# select * from collection;

 database |       name        |        identifier        
----------+-------------------+--------------------------
 stampede | primer            | stampede_primer
```

##### doc_part

The `doc_part` table stores the mappings for the different levels in the JSON documents. As we have seen above, each level (also the root level) corresponds to one table. The key to this table is the MongoDB `database` and `collection` names along with the key path (column `table_ref`). The key path of the root level is the empty array (`{}`), which is mapped to the PostgreSQL table name `primer` (again, column `identifier`). For subdocuments, the `table_ref` column is the key path as array.

```no-highlight
# select * from doc_part;

 database |    collection     |        table_ref        |               identifier                | last_rid
----------+-------------------+-------------------------+-----------------------------------------+----------
 stampede | primer            | {}                      | primer                                  |        0
 stampede | primer            | {address}               | primer_address                          |        0
 stampede | primer            | {grades}                | primer_grades                           |        0
 stampede | primer            | {address,coord}         | primer_address_coord                    |        0
```

##### field

The `field` table follows the same logic: the columns `database`, `collection`, and `table_ref` are used to identify a flat JSON level. The column `name` is the field name in the JSON document. The table provides the type as well as the SQL column name (`identifier`, which has the [type postfix](#column-type-encoding) as well).

```no-highlight
# select * from field;

 database |    collection     |        table_ref        |         name          |      type       |       identifier        
----------+-------------------+-------------------------+-----------------------+-----------------+-------------------------
 stampede | primer            | {}                      | address               | CHILD           | address_e
 stampede | primer            | {}                      | restaurant_id         | STRING          | restaurant_id_s
 stampede | primer            | {}                      | name                  | STRING          | name_s
 stampede | primer            | {}                      | cuisine               | STRING          | cuisine_s
 stampede | primer            | {}                      | _id                   | MONGO_OBJECT_ID | _id_x
 stampede | primer            | {}                      | borough               | STRING          | borough_s
 stampede | primer            | {}                      | grades                | CHILD           | grades_e
 stampede | primer            | {address}               | zipcode               | STRING          | zipcode_s
 stampede | primer            | {address}               | coord                 | CHILD           | coord_e
 stampede | primer            | {address}               | street                | STRING          | street_s
 stampede | primer            | {address}               | building              | STRING          | building_s
 stampede | primer            | {grades}                | date                  | INSTANT         | date_t
 stampede | primer            | {grades}                | score                 | INTEGER         | score_i
 stampede | primer            | {grades}                | grade                 | STRING          | grade_s
 stampede | primer            | {grades}                | score                 | NULL            | score_n
```

##### scalar

Finally, there is one more table needed to store the type of array elements—because JSON arrays can contain mixed type elements. In this case, the respective table (e.g. [`primer_address_coord`](#primer_address_coord) well have multiple data columns).

```no-highlight
# select * from scalar;

 database | collection |    table_ref    |  type  | identifier
----------+------------+-----------------+--------+------------
 stampede | primer     | {address,coord} | DOUBLE | v_d
```

## Column Type Encoding

[TODO]: <> (explain the possible values of `_e`)

The following table lists the type postfixes used by ToroDB Stampede.

| Postfix | What does it mean? |
|---------|--------------------|
| _a | This represents MongoDB's MAX_KEY type, stored with a true value. |
| _b | Boolean value, stored as a boolean in PostgreSQL. |
| _c | A date (with time) value in format ISO-8601, stored with PostgreSQL type date. |
| _d | A 64-bit IEEE 754 floating point, stored with PostgreSQL type double precision. |
| _e | A child element, it can be an object or an array, stored with PostgreSQL type boolean with a value of false to indicate a child object and true to indicate a child array. |
| _g | A PostgreSQL jsonb type, composed of two strings meaning the pattern and the evaluation options for a RegEx in MongoDB's style. |
| _i | A 32-bit signed two's complement integer, stored with PostgreSQL type integer. |
| _j | This represents the MONGO_JAVASCRIPT type, stored with PostgreSQL type character varying. |
| _k | This represents MongoDB's MIN_KEY type, stored with a false value. |
| _l | A 64-bit signed two's complement integer, stored with PostgreSQL type bigint. |
| _m | A time value in format ISO-8601, stored with PostgreSQL type time. |
| _n | A null value, stored with PostgreSQL type boolean (nullable). It cannot take value false, just true or null. When the value is true means the JSON document has value null for that path, when it is null it means the path has another value or does not exist for that document. |
| _p | This represents the MONGO_DB_POINTER type, and it is stored as a PostgreSQL jsonb, composed of two strings meaning the namespace and the objectId. |
| _q | This represents MongoDB's Decimal128 type. It's stored as a PostgreSQL type containing a numeric value and three booleans that specify whether the value is or isn't infinite, NaN or negative zero. |
| _r | Binary object, it is an array of bytes stored in PostgreSQL as bytea. |
| _s | An array of UTF-8 characters representing a text, stored with PostgreSQL type character varying. |
| _t | Number of milliseconds from 1970-01-01T00:00:00Z, stored with PostgreSQL type timestamptz. |
| _u | This represents the undefined type, stored with a true value. |
| _w | This represents the MONGO_JAVASCRIPT_WITH_SCOPE type, stored with PostgreSQL type jsonb. |
| _x | This represents the MONGO_OBJECT_ID and it is stored as a PostgreSQL bytea. |
| _y | This represents the MONGO_TIMESTAMP and it is stored as a PostgreSQL composite type formed by an integer column secs and an integer column counter. |
| _z | This represents the DEPRECATED type. We assign a String to represent it, so it is stored with PostgreSQL type character varying. |

__Notes about MONGO_OBJECT_ID__: ObjectIds are small, likely unique, fast to generate, and ordered. ObjectId values consists of 12-bytes, where the first four bytes are a timestamp that reflect the ObjectId’s creation, specifically:

* 4-byte value representing the seconds since the Unix epoch,
* 3-byte machine identifier,
* 2-byte process id, and
* 3-byte counter, starting with a random value.

### Data Conflict Resolution

MongoDB does not have type constraints on collections: a JSON element that is a string in one document can be an integer in the next document of the same collection (but still use the same key). In this case, ToroDB Stampede creates multiple columns for this key: at least one for each type (and possibly one more to indicate null).

For example, in the `primer_grades` table there are two different columns for the `score` key. One is `score_i` to store integer values and the another is `score_n` to store if the value contains null in the original document. The extra column is needed to store whether that the `score` key is missing or it had the null value.

```no-highlight
did   |  rid  | seq |         date_t         | score_i |    grade_s     | score_n
------+-------+-----+------------------------+---------+----------------+---------
    0 |     0 |   0 | 2014-08-21 02:00:00+02 |       6 | A              |
    0 |     1 |   1 | 2014-02-03 01:00:00+01 |      19 | B              |
    0 |     2 |   2 | 2013-04-13 02:00:00+02 |       7 | A              |
    0 |     3 |   3 | 2012-10-17 02:00:00+02 |       9 | A              |
    0 |     4 |   4 | 2011-10-22 02:00:00+02 |      10 | A              |
    1 |     5 |   0 | 2014-02-20 01:00:00+01 |      10 | A              |
    1 |     6 |   1 | 2013-07-22 02:00:00+02 |      12 | A              |
    1 |     7 |   2 | 2012-06-25 02:00:00+02 |      10 | A              |
    1 |     8 |   3 | 2011-11-16 01:00:00+01 |      22 | B              |
    1 |     9 |   4 | 2011-04-26 02:00:00+02 |      12 | A              |
 7148 | 34375 |   0 | 2015-01-20 01:00:00+01 |         | Not Yet Graded | t
22559 | 91238 |   0 | 2015-01-20 01:00:00+01 |         | Not Yet Graded | t
23204 | 91961 |   0 | 2015-01-20 01:00:00+01 |         | Not Yet Graded | t
23392 | 92137 |   0 | 2015-01-20 01:00:00+01 |         | Not Yet Graded | t

```

The rows with value `true` for column `score_n` means the associated JSON document had a value null for path `score` like it is shown in the next example.

```
{
    "address": {
        "building": "725",
        "coord": [-74.01381169999999, 40.6336821],
        "street": "65 Street",
        "zipcode": "11220"
    },
    "borough": "Brooklyn",
    "cuisine": "Other",
    "grades": [{
        "date": {
            "$date": 1421712000000
        },
        "grade": "Not Yet Graded",
        "score": null
    }],
    "name": "Swedish Football Club",
    "restaurant_id": "41278206"
}
```

## Example Queries

You can query the data in the relational schema like any other PostgreSQL database: using the `psql` command, or tools like [PgAdmin](https://www.pgadmin.org/).

### List Bakeries in Given ZIP Code

```no-highlight
select p.name_s
from primer p
join primer_address pa ON (p.did = pa.did)
where  p.cuisine_s = 'Bakery'
  and pa.zipcode_s = '10462'
```

```no-highlight
               name_s                
-------------------------------------
 Morris Park Bake Shop
 Zaro'S Bread Basket
 Ronald Pitusa Bakery
 National Bakery
 Conti'S Pastry Shoppe
 Gina'S Italian Bakery & Pastry Shop
 Mr Cake Bakery & Dessert
```

### Simple Analytics

One of the advantages of a relational model is the ability to run complex queries efficiently.

The next query extends the previous query to also show the average score of each bakery. For that, it joins the `primer_grades` table and groups by the bakery name to be build the average.

```no-highlight
select p.name_s, avg(pg.score_i)
from primer p
join primer_address pa ON (p.did = pa.did)
join primer_grades pg  ON (p.did = pg.did)
where  p.cuisine_s = 'Bakery'
  and pa.zipcode_s = '10462'
group by p.name_s
```

```no-highlight
               name_s                |         avg         
-------------------------------------+---------------------
 Conti'S Pastry Shoppe               | 10.3333333333333333
 Gina'S Italian Bakery & Pastry Shop | 18.3333333333333333
 Zaro'S Bread Basket                 |  8.1666666666666667
 Mr Cake Bakery & Dessert            | 12.0000000000000000
 Ronald Pitusa Bakery                |  9.0000000000000000
 Morris Park Bake Shop               |  8.2000000000000000
 National Bakery                     | 12.4000000000000000
```

### Further Filtering

Of course it is possible to limit the result to those bakeries that have an average score greater than ten (10): just add a `having` clause.

```no-highlight
select p.name_s, avg(pg.score_i)
from primer p
join primer_address pa ON (p.did = pa.did)
join primer_grades pg  ON (p.did = pg.did)
where  p.cuisine_s = 'Bakery'
  and pa.zipcode_s = '10462'
group by p.name_s
having avg(pg.score_i) > 10
```

```no-highlight
               name_s                |         avg         
-------------------------------------+---------------------
 Conti'S Pastry Shoppe               | 10.3333333333333333
 Gina'S Italian Bakery & Pastry Shop | 18.3333333333333333
 Mr Cake Bakery & Dessert            | 12.0000000000000000
 National Bakery                     | 12.4000000000000000
```

### Full PostgreSQL Query Capabilities

The examples above are just the tip of the iceberg. With PostgreSQL, you can use many advanced query techniques including [window functions](https://www.postgresql.org/docs/current/static/tutorial-window.html) and [common table expressions](https://www.postgresql.org/docs/current/static/queries-with.html). You can now answer pretty much any question with a single query. You can also use all [PostgreSQL index types](https://www.postgresql.org/docs/current/static/indexes-types.html) to tune them.
