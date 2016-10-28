# What is ToroDB Stampede?

Connected to a MongoDB replica set, ToroDB Stampede is able to replicate the NoSQL data into a relational backend (right now the only available backend is PostgreSQL) using the oplog.

![ToroDB Stampede Structure](images/toro_stampede_structure.jpg)

Existen algunas soluciones que hacen tareas similares usando el soporte JSON de PostgreSQL, pero siguen teniendo problemas a la hora de utilizar los datos y extraer información de ellos. Lo que hace ToroDB Stampede es replicar la estructura del documento en tablas relacionales y luego almancenar las tuplas de datos correspondientes al documento en esas tablas.

![Mapping example](images/toro_stampede_mapping.jpg)

Teniendo esta estructura relacional se evitan ciertos problemas de las soluciones NoSQL, como por ejemplo la realización de queries agregadas en un tiempo aceptable.

# ToroDB Stampede limitations

No todo podía ser perfecto, y hay una serie de limitaciones conocidas de ToroDB Stampede.

* Actualmente la versión soportada de MongoDB es la 3.2.
* No está soportado el uso de [capped collections](https://docs.mongodb.com/manual/core/capped-collections/).
* Si se hace uso del caracter `\0` se escapará debido a que PostgreSQL no soporta el uso de dicho caracter dentro de una cadena de texto.
* Si se recibe el comando `applyOps` se para el servidor de replicación.
* Si se recibe el comando `collmod` este será ignorado.
* Actualmente no está soportado el uso de índices.
* Actualmente no está soportado el uso en entornos de MongoDB configurados con sharding.
* No se soportan ciertos operandos de update.

[TODO]: <> (no sabemos si por el oplog llegan los operandos de update que no soportamos)
[TODO]: <> (tipos no soportados, hay que hacer una lista)

[Versions]: <> (Esta sección no tiene sentido ahora mismo)

# Documentation conventions

# Glossary

| Term | Definition |
|------|------------|
| __capped collection__ | A fixed-sized collection that automatically overwrites its oldest entries when it reaches its maximum size. |
| __oplog__ | A capped collection that stores an ordered history of logical writes to a MongoDB database. The oplog is the basic mechanism enabling replication in MongoDB.|
| __path__ | (JSON document path) The required sequence of keys to access one specific value of the document (for example, address.zipcode) | 
| __replica set__ | A replica set is the way provided by MongoDB to add redundancy to survive network partitions and other technical failures ([more info](https://docs.mongodb.com/manual/tutorial/deploy-replica-set/)) |
| __sharding__ | When the amount of data is too large it is recommended to distribute the information across multiple machines, this is known as sharding in MongoDB ([more info](https://docs.mongodb.com/v3.2/sharding/)) |
