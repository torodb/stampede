# Previous requirements installation

The execution of ToroDB Stampede depends on a number of prerequisites, in the next table more information on how to install and manage them is provided.

| | Decription | External links |
|-|------------|----------------|
| MongoDB | It is the NoSQL system where the original data is stored and the replication data source. | [more info](https://docs.mongodb.com/manual/installation/) |
| MongoDB replica set | ToroDB Stampede is designed to replicate from a primary node of a MongoDB replica set. | [more info](https://docs.mongodb.com/manual/tutorial/deploy-replica-set/) | 
| PostgreSQL | ToroDB Stampede correct operation relies on the existence of a backend, right now it should be PostgreSQL. | [more info](https://wiki.postgresql.org/wiki/Detailed_installation_guides) |
| Java | Como ToroDB Stampede ha sido desarrollado en Java se requiere una máquina virtual Java para su correcta ejecución. | [more info](https://java.com/en/download/help/index_installing.xml) |

Among the previous requisites, if we want to compile or execute in a virtual containers environment other requisites are mandatory.

| | Decription | External links |
|-|------------|----------------|
| Docker | Docker is a virtual container environment that allows to deploy infrastructure in a safe and easy way. | [more info](https://docs.docker.com/engine/installation/) |
| Maven | Maven es el sistema de construcción y gestión de dependencias utilizado, por lo que es necesario en caso de querer compilar el código fuente. | [more info](http://maven.apache.org/install.html) | 

# Executing ToroDB Stampede

## Docker

### Mac

#### A partir del código fuente

Si queremos desplegar ToroDB Stampede en Docker haciendo uso de las herramientas provistas en el código fuente basta con ejecutar los siguientes comandos Maven.

```
$ mvn clean package -P docker -Ddocker.skipbase=false

$ mvn -f packaging/stampede/main/pom.xml -P docker-stampede-fullstack docker:run -Ddocker.follow
```

Si se produjese algún error en la construcción, se recomienda ejecutar el empaquetado deshabilitando la cache de Docker.

```
$  mvn clean package -P docker -Ddocker.skipbase=false -Ddocker.nocache=true
```

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
