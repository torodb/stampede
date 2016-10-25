# Advanced concepts

Para comprender mejor la naturaleza del algoritmo de mapeo de documento JSON a un almacenamiento relacional, se ejecutará un ejemplo de mapeo usando el ejemplo [primer de MongoDB](https://docs.mongodb.com/getting-started/shell/import-data/).

Suponiendo que tenemos ToroDB Stampede replicando de un MongoDB en replica set, importaremos los datos del dataset en MongoDB para que se repliquen en PostgreSQL.

```
$ wget https://www.dropbox.com/s/570d4tyt4hpsn03/primer-dataset.json?dl=0

$ mongoimport -d stampede -c primer primer-dataset.json
```

Como se puede observar se ha hecho la importación en la base de datos stampede y la colección primer. En PostgreSQL esto significa que se ha creado dentro de la base de datos torod, el esquema stampede con una tabla raíz primer y una serie de tablas denominadas primer_*.

## Table mapping

Sabiendo que la estructura de los documentos JSON que contiene el dataset es equivalente a la siguiente.

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

Se crearían un total de 4 tablas que corresponden a los diferentes paths del documento que tienen como hijos un subdocumento.

### primer

```
did  | address_e | restaurant_id_s |                                               name_s                                               |                            cuisine_s                             |           _id_x            |   borough_s   | grades_e
-------+-----------+-----------------+----------------------------------------------------------------------------------------------------+------------------------------------------------------------------+----------------------------+---------------+----------
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

```

### primer_address_coord

### primer_grades

![Table mapping](images/relational_structure.jpg)

## Data conflict resolution
