El siguiente ejercicio servirá para explicar como se mapean documentos de MongoDB en PostgreSQL usando Toro Stampede.

[TOC]

## 1. Arrancar MongoDB en replicación

## 2. Arrancar Toro Stampede

## 3. Importar documentos en MongoDB

## Estructura de tablas en PostgreSQL

El dataset utilizado para el ejemplo, es un listado de restaurantes con sus correspondientes evaluaciones, y la estructura de los documentos es como la indicada a continuación.

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

Para este ejemplo, se crearán 3 tablas diferentes:
* books: Corresponde con el nombre de la colección utilizada porque se trata del nivel raíz del documento.
* books_address: Es la tabla que guarda los datos del path `address` del documento.
* books_grades: Es la tabla que guarda los datos del path `grades` del documento y que corresponde con un array.

## Metadatos
