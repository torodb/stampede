# Previous requirements installation

The execution of ToroDB Stampede depends on a number of prerequisites, in the next table more information on how to install and manage them is provided.

| | Decription | External links |
|-|------------|----------------|
| MongoDB | It is the NoSQL system where the original data is stored and the replication data source. | [more info](https://docs.mongodb.com/manual/installation/) |
| MongoDB replica set | ToroDB Stampede is designed to replicate from a primary node of a MongoDB replica set. | [more info](https://docs.mongodb.com/manual/tutorial/deploy-replica-set/) | 
| PostgreSQL | ToroDB Stampede correct operation relies on the existence of a backend, right now it should be PostgreSQL. | [more info](https://wiki.postgresql.org/wiki/Detailed_installation_guides) |
| Java | Como ToroDB Stampede ha sido desarrollado en Java se requiere una máquina virtual Java para su correcta ejecución. | [more info](https://java.com/en/download/help/index_installing.xml) |

Además de los requisitos anteriores, si queremos trabajar con el código fuente hay otros requisitos.

| | Decription | External links |
|-|------------|----------------|
| Maven | Maven es el sistema de construcción y gestión de dependencias utilizado, por lo que es necesario en caso de querer compilar el código fuente. | [more info](http://maven.apache.org/install.html) | 

# Executing ToroDB Stampede

# Docker

## Binary distribution

### Linux/Mac

Antes de arrancar ToroDB Stampede será necesario tener una instancia correctamente configurada de PostgreSQL y un MongoDB configurado como replica set.

[//]: # (Indicar dónde se puede descargar el fichero binario para MacOS)

```
$ tar xjvf <stampede-binary>.tar.bz2

$ torodb-stampede-<version>/bin/torodb-stampede
```

## Source code

# Configuración básica de ToroDB

## Config file

## CLI parameters

# Configuración avanzada

## Config file

## CLI parameters
