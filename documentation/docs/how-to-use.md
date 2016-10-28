Para comprender mejor la naturaleza del algoritmo de mapeo de documentos JSON a un almacenamiento relacional, se realizará un ejemplo real usando el mismo [dataset](https://docs.mongodb.com/getting-started/shell/import-data/) que utiliza MongoDB en su documentación.

Suponiendo que tenemos ToroDB Stampede replicando de un MongoDB, importaremos los datos en MongoDB para que se repliquen en formato relacional en PostgreSQL. Para ello basta ejecutar los siguientes comandos.

```
$ wget https://www.dropbox.com/s/570d4tyt4hpsn03/primer-dataset.json?dl=0

$ mongoimport -d stampede -c primer primer-dataset.json
```

Como se puede observar se ha hecho la importación en la base de datos con nombre `stampede` y la colección `primer`, esto es importante de cara al esquema y nombres de tablas que se van a utilizar. En PostgreSQL esto significa que se ha creado dentro de la base de datos `torod`, el esquema `stampede` con una tabla raíz `primer` y una serie de tablas denominadas `primer_*`.

# Table mapping

En esencia, cada nivel del documento JSON se mapea a una tabla diferente en el backend relacional. Por tanto, sabiendo que la estructura de los documentos JSON que contiene el dataset es equivalente a la siguiente.

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

Se crearían un total de 4 tablas que corresponden a los diferentes niveles del documento. Es decir, una tabla inicial para la raíz del documento que se llama `primer`, porque es el nombre de colección seleccionado, y otras tres tablas que corresponden con los paths de los subdocumentos encontrados.

* primer_address
* primer_address_coord
* primer_grades

![Tables distribution](images/tables_distribution.jpeg)

## Created tables

### primer

Como se ha indicado, la raíz del documento se mapea a una tabla que tiene como nombre el que se haya usado en la colección de MongoDB, en este caso `primer`.

A su vez, cada elemento del nivel raíz se mapea a una columna, ya sea este un tipo de dato escalar o un subdocumento. En la siguiente sección se puede consultar el tipo de datos que existen y como son mapeados a columnas en el backend relacional.

```
did  | address_e | restaurant_id_s |                                               name_s                                               |                            cuisine_s                             |           _id_x            |   borough_s   | grades_e
-----+-----------+-----------------+----------------------------------------------------------------------------------------------------+------------------------------------------------------------------+----------------------------+---------------+----------
   0 | f         | 40384115        | Phil & Sons Restaurant & Pizzeria                                                                  | Pizza/Italian                                                    | \x580f12efbe6e3fff2237caef | Queens        | t
   1 | f         | 40384100        | Josie'S Restaurant                                                                                 | American                                                         | \x580f12efbe6e3fff2237caee | Manhattan     | t
   2 | f         | 40384036        | Mcdonald'S                                                                                         | Hamburgers                                                       | \x580f12efbe6e3fff2237caed | Brooklyn      | t
   3 | f         | 40383945        | Pizza D'Oro                                                                                        | Pizza                                                            | \x580f12efbe6e3fff2237caec | Staten Island | t
   4 | f         | 40383931        | Flash Dancers                                                                                      | American                                                         | \x580f12efbe6e3fff2237caeb | Manhattan     | t
   5 | f         | 40383836        | Barone Pizza                                                                                       | Pizza                                                            | \x580f12efbe6e3fff2237caea | Queens        | t
   6 | f         | 40383825        | Russian Samovar                                                                                    | Russian                                                          | \x580f12efbe6e3fff2237cae9 | Manhattan     | t
   7 | f         | 40383819        | Short Stop Restaurant                                                                              | American                                                         | \x580f12efbe6e3fff2237cae8 | Bronx         | t
```

### primer_address

```
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

### primer_address_coord

La tabla `primer_address_coord` es un caso especial, al igual que `primer_grades`, porque es un path que referencia a un array, por esta razón la columna `seq` posee valores que indican la posición del elemento en el array. Para comprender mejor las columnas de metadatos utilizadas en las tablas se puede leer el apartado de [metada](how-to-use.md#metadata).

```
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

### primer_grades

```
did  |  rid  | seq |         date_g         | score_i |    grade_s     | score_n
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

# Columns and metadata

[TODO]: <> (Explicar los posibles valores de los campos tipo e)
[TODO]: <> (Revisar que estén todos los tipos de datos ... BINARY, BOOLEAN, DATE, DOUBLE, INSTANT, INTEGER, LONG, MONGO_OBJECT_ID, MONGO_TIME_STAMP, NULL, STRING, TIME, CHILD)

Como se puede observar en los extractos de las tablas creadas, los nombres de columnas incluyen un postfijo, que indica el tipo de dato. Como en JSON no hay restricciones a la hora de asignar valores a un campo, pero en un backend relacional sí, se ha decidido crear una columna para cada tipo de dato diferente asignado al mismo path (ver la sección [Data conflict resolution](how-to-use.md#data-conflict-resolution)).

Los diferentes tipos de datos que maneja ToroDB Stampede se representan en la siguiente tabla.

| Postfix | What does it mean? |
|---------|--------------------|
| _b | Boolean value, store like boolean in PostgreSQL. |
| _c | A date (with time) value in format ISO-8601, stored with PostgreSQL type date. |
| _d | A 64-bit IEEE 754 floating point, stored with PostgreSQL type double precision. |
| _e | A child element, it can be an object or an array, stored with PostgreSQL type boolean with a value of false for object and true for array. |
| _i | A 32-bit signed two's complement integer, stored with PostgreSQL type integer. |
| _l | A 64-bit signed two's complement integer, stored with PostgreSQL type bigint. |
| _n | A null value, stored with PostgreSQL type boolean (nullable). It cannot take value false, just true or null. When the value is true means the JSON document has value null for that path, when it is null and the associated column when it has value is null too, it means the path does not exist for that document. |
| _m | A time value in format ISO-8601, stored with PostgreSQL type time. |
| _r | Binary object, it is an array of bytes stored in PostgreSQL as bytea. |
| _s | An array of UTF-8 characters representing a text, stored with PostgreSQL type character varying. |
| _t | Number of milliseconds from 1970-01-01T00:00:00Z, stored with PostgreSQL type timestamptz. |
| _x | This represent the MONGO_OBJECT_ID and it is stored as a PostgreSQL bytea. |

__Notes about MONGO_OBJECT_ID__: ObjectIds are small, likely unique, fast to generate, and ordered. ObjectId values consists of 12-bytes, where the first four bytes are a timestamp that reflect the ObjectId’s creation, specifically:

* 4-byte value representing the seconds since the Unix epoch,
* 3-byte machine identifier,
* 2-byte process id, and
* 3-byte counter, starting with a random value.

## Data conflict resolution

Por la propia naturaleza de los documentos JSON puede ocurrir que un mismo path tenga dos tipos de datos diferentes, o que en algunos documentos ese path no exista. No es un problema para un documento JSON, pero sí lo es para un sistema relacional en el que cada columna tiene asociado un determinado tipo de dato.

Para resolver este problema en ToroDB Stampede, se ha decidido crear una columna diferente para cada tipo de dato. Por ejemplo, en el extracto de la tabla `primer_grades` del ejemplo anterior, existen dos columna diferentes para el valor de la clave `score`. La columna `score_i` almacena los datos que son de tipo entero, mientras que la columna `score_n` indica cuando ese path tiene valor nulo o ni siquiera se ha especificado el path.

Para que se comprenda mejor, a continuación se muestra un extracto de elementos en `primer_grades` que tienen valor para `score_i` y otros que no, pero que sí tienen valor para `score_n`.

```
did   |  rid  | seq |         date_g         | score_i |    grade_s     | score_n
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

En las filas que tienen valor `true` para la columna `score_n` significa que en el documento JSON asociado, el valor para `score` era `null`. Por ejemplo:

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

## Metadata

Anteriormente se ha comentado que ToroDB Stampede almacena una serie de metadatos que le permiten gestionar el documento, recomponerlo o hacer diferentes tipos de búsquedas. Estos metadatos se diferencian entre una serie de columnas creadas en las propias tablas de datos y una serie de tablas específicas de uso interno.

### Columnas de metadatos

ToroDB Stampede crea varias columnas de metadatos en las tablas que sirven para mantener y recomponer los documentos originales. Además servirán al usuario para poder hacer consultas complejas de los datos.

| Columna | ¿Para qué sirve? |
|---------|------------------|
| did | Representa el identificador único del documento y posee el mismo valor para todas las filas correspondientes a ese documento. |
| rid | Es un identificador único de la fila, por ejemplo, en el caso de arrays podemos encontrar varias filas con el mismo `did`, pero el `rid` es único para cada una. __Cabe señalar que la tabla raíz no tiene `rid` porque coincide con el `did`__. |
| pid | Es la referencia al `rid` del elemento padre. Por ejemplo `primer_address_coord` posee los elementos con `rid` 0 y 1 que tienen como `pid` el valor 0, eso significa que son hijos de la fila de `primer_address` con `rid` 0. |
| seq | Es un valor que sólo se utiliza en el caso de los arrays y que representa la posición del elemento en el array. |

![PID reference](images/pid_reference.jpeg)

### Tablas de metadatos

Las columnas de metadatos en las tablas de replicación no son suficientes para mantener el sistema, por lo que además también existen una serie de tablas de metadatos específicas que se guardan en el esquema `torod`.

#### database

La tabla `database` almacena el nombre utilizado por el usuario al crear la base de datos en MongoDB, lo cual se traduce en PostgreSQL en un esquema. Como el nombre de esquema tiene limitaciones de tamaño, se utiliza la tabla `database` para guardar el nombre usado por el usuario y el identificador que se usará en PostgreSQL, aunque en general serán equivalentes.

```
# select * from database;

   name   | identifier
----------+------------
 stampede | stamped
```

#### collection

Además del nombre de base de datos, en MondoDB hay que especificar en qué colección se guardan los datos, esta referencia es almacenada en la tabla `collection`.

```
# select * from collection;

 database |       name        |        identifier        
----------+-------------------+--------------------------
 stampede | primer            | stampede_primer
```

#### doc_part

Como ya hemos indicado previamente, el nombre de la tabla para el elemento raíz de un documento JSON es el mismo que se ha indicado como nombre de la colección de MongoDB. En ToroDB Stampede, el `table_ref` asociado a ese elemento es `{}` y su identificador, en este caso, es `primer`.

Si, en cambio, hablamos del path `address.coord`, el table ref será `{address,coord}`, mientras que el identificador de la tabla será `primer_address_coord`.

```
# select * from doc_part;

 database |    collection     |        table_ref        |               identifier                | last_rid
----------+-------------------+-------------------------+-----------------------------------------+----------
 stampede | primer            | {}                      | primer                                  |        0
 stampede | primer            | {address}               | primer_address                          |        0
 stampede | primer            | {grades}                | primer_grades                           |        0
 stampede | primer            | {address,coord}         | primer_address_coord                    |        0
```

#### field

La tabla `field` almacena el tipo de dato de cada columna y su identificador. Nuevamente, cabe destacar que el nombre utilizado en la columna no siempre coincidirá con el nombre original en el documento, si se han usado nombres de clave muy largos.

Por tanto para una determinada combinación de `database, collection, table_ref`, se guarda el nombre de clave usado en el documento original, el nombre asignado a la columna y el tipo de datos que almacena. Este tipo de dato puede ser un tipo escalar, como `string` o `double` o puede ser un tipo `child` en cuyo caso hará referencia a una nueva tabla.

```
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
 stampede | primer            | {grades}                | date                  | INSTANT         | date_g
 stampede | primer            | {grades}                | score                 | INTEGER         | score_i
 stampede | primer            | {grades}                | grade                 | STRING          | grade_s
 stampede | primer            | {grades}                | score                 | NULL            | score_n
```

#### scalar

Por último la tabla `scalar` es utilizada para guardar el tipo de datos de los elementos de un array. Hay que tener en cuenta que en general para un mismo array todos los elementos serán del mismo tipo, pero no hay nada que impida que un documento JSON no contenga elementos de distintos tipos en un array.

En el caso del ejemplo utilizado, sólo contiene una fila que indica que los elementos del array en el path `address.coord` son de tipo `double`. Esto se traduce en que el tipo de la columna `v_d` de la tabla `stampede_address_coord` es `double`.

```
# select * from scalar;

 database | collection |    table_ref    |  type  | identifier
----------+------------+-----------------+--------+------------
 stampede | primer     | {address,coord} | DOUBLE | v_d
```

# Example queries

Supongamos que queremos hacer consultas sobre los datos replicados, como cabe esperar se puede usar la propia consola de PostgreSQL o cualquier herramienta que haga uso de su conector.

Por ejemplo, si queremos el nombre de todos los locales que son panaderías en el código postal 10462, ejecutaríamos la siguiente query.

```
select p.name_s from primer p, primer_address pa
where
  p.cuisine_s = 'Bakery'
  and p.did = pa.did
  and pa.zipcode_s = '10462'
```

```
# select p.name_s from primer p, primer_address pa where p.cuisine_s = 'Bakery' and p.did = pa.did and pa.zipcode_s = '10462';

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

Una de las ventajas principales de tener los datos en formato relacional es que se puedenh realizar queries complejas de forma más sencilla y rápida. Por ejemplo si queremos conocer la nota media de cada una de las panaderías podríamos ejecutar la query siguiente.

```
select p.name_s, avg(pg.score_i)
from primer p, primer_address pa, primer_grades pg
where
  p.cuisine_s = 'Bakery'
  and p.did = pa.did
  and pa.zipcode_s = '10462'
  and pg.did = p.did
group by p.name_s
```

```
# select p.name_s, avg(pg.score_i) from primer p, primer_address pa, primer_grades pg where p.cuisine_s = 'Bakery' and p.did = pa.did and pa.zipcode_s = '10462' and pg.did = p.did group by p.name_s;

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

Y ahora sería muy senillo filtrar aquellas que tengan un nota media superior a 10.

```
select p.name_s, avg(pg.score_i)
from primer p, primer_address pa, primer_grades pg
where
  p.cuisine_s = 'Bakery'
  and p.did = pa.did
  and pa.zipcode_s = '10462'
  and pg.did = p.did
group by p.name_s
having avg(pg.score_i) > 10
```

```
# select p.name_s, avg(pg.score_i) from primer p, primer_address pa, primer_grades pg where p.cuisine_s = 'Bakery' and p.did = pa.did and pa.zipcode_s = '10462' and pg.did = p.did group by p.name_s having avg(pg.score_i) > 10;

               name_s                |         avg         
-------------------------------------+---------------------
 Conti'S Pastry Shoppe               | 10.3333333333333333
 Gina'S Italian Bakery & Pastry Shop | 18.3333333333333333
 Mr Cake Bakery & Dessert            | 12.0000000000000000
 National Bakery                     | 12.4000000000000000
```
