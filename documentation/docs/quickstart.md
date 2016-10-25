# Quickstart

## Requisitos previos

Para el correcto funcionamiento de ToroDB Stampede serán necesarios una serie de requisitos previos:

* Una instancia de MongoDB configurada como nodo primario de un replica set.
* Una instancia de PostgreSQL para servir como backend relacional de ToroDB Stampede.
* Una máquina virtual Java 8.

En caso de que no se cumpla alguno de los requisitos, se puede encontrar más documentación al respecto en el siguiente [enlace](installation.md) de la documentación.

## Configuración del backend

Para poder hacer la replicación de forma correcta, ToroDB Stampede necesita un backend correctamente configurado. Es decir, una base de datos que haga de motor de persistencia de los datos que se desean replicar, en este caso PostgreSQL.

Suponiendo que tenemos una instalación correcta de PostgreSQL, hay que realizar dos tareas.

* Crear el rol `torodb` con permisos para crear bases de datos y hacer login.
* Crear la base de datos `torod` con owner `torodb`.

```
$ sudo -u postgres createuser -P --interactive

$ sudo -u postgres createdb -O torodb torod

$ sudo adduser torodb
```

Ahora ya se puede acceder a la nueva base de datos que se ha creado, que será la que utilice Stampede para hacer la replicación.

```
$ sudo -u torodb psql
```

## Arrancar el binario de ToroDB Stampede

Para ejecutar ToroDB Stampede bastará con descargar el fichero binario de la siguiente [URL](https://www.dropbox.com/s/54eyp7jyu8l70aa/torodb-stampede-0.50.0-SNAPSHOT.tar.bz2?dl=0), descomprimirlo y ejecutarlo pasándole como argumento un fichero con la configuración de PostgreSQL.

Creamos el fichero de configuración de credenciales de PostgreSQL, que sigue el formato del fichero .pgpass. El formato correcto es una o más líneas con la siguiente estructura, <host>:<port>:<database>:<user>:<password>. Por ejemplo.

```
localhost:5432:torod:torodb:torodb
```

Una vez que tenemos el fichero creado procedemos a arrancar ToroDB Stampede.

```
$ wget https://www.dropbox.com/s/54eyp7jyu8l70aa/torodb-stampede-0.50.0-SNAPSHOT.tar.bz2?dl=0

$ tar xjvf <stampede-binary>.tar.bz2

$ torodb-stampede-<version>/bin/torodb-stampede --toropass-file <postgres-credentials>
```

Ahora deberíamos tener ToroDB Stampede activo y replicando las operaciones que se realicen contra MongoDB.

## Ejemplo de mapeo

Para tener un mejor ejemplo de como funciona Stampede, vamos a hacer un pequeño ejercicio. En este ejercicio importaremos una colección en MongoDB y veremos como los datos quedan replicados en PostgreSQL haciendo uso de Stampede.

Suponiendo que tenemos Stampede corriendo correctamente, después de haber ejecutado los pasos anteriores, vamos a importar en Mongo el dataset que se encuentra en la siguiente [URL](https://www.dropbox.com/s/570d4tyt4hpsn03/primer-dataset.json?dl=0).

```
$ wget https://www.dropbox.com/s/570d4tyt4hpsn03/primer-dataset.json?dl=0

$ mongoimport -d stampede -c primer primer-dataset.json
```

Ahora nos podemos conectar a PostgreSQL y comprobar que ha creado una nueva estructura de tablas en el esquema `stampede`, esto se debe a que se ha seleccionado como base de datos en el mongoimport este nombre.

```
$ sudo -u torodb psql torod

# set schema 'stampede'
```

Si verificamos la estructura de tablas creada deberías tener algo similar a la siguiente figura.

```
torod=# \d
                List of relations
  Schema  |         Name         | Type  | Owner  
----------+----------------------+-------+--------
 stampede | primer               | table | torodb
 stampede | primer_address       | table | torodb
 stampede | primer_address_coord | table | torodb
 stampede | primer_grades        | table | torodb
(4 rows)
```

Para entender mejor como se realiza el mapeo de documentos JSON a tablas en una base de datos relacional, se puede consultar el siguiente [enlace](advanced.md) de la documentación.
