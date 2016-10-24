# Installation and configuration

## Instalación de los requisitos previos

### MongoDB Community Edition

#### Ubuntu 16.04 LTS

En esta sección se decriben los pasos a seguir para instalar MongoDB Community Edition en un Ubuntu 16.04 LTS, si fuese necesario se puede encontrar más información en la [documentación oficial de MongoDB](https://docs.mongodb.com/v3.2/tutorial/install-mongodb-on-ubuntu/).

Importar la clave pública utilizada por el gestor de paquetes de Ubuntu.

```
$ sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv EA312927
```

Crear la lista de fuentes para MongoDB.

```
$ echo "deb http://repo.mongodb.org/apt/ubuntu xenial/mongodb-org/3.2 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb-org-3.2.list
```

Actualizar el listado de paquetes del sistema.

```
$ sudo apt-get update
```

Instalar el paquete de MongoDB Community Edition.

```
$ sudo apt-get install -y mongodb-org
```

Crear el fichero `/lib/systemd/system/mongod.service`. __Sólo para Ubuntu 16.04__.

```
[Unit]
Description=High-performance, schema-free document-oriented database
After=network.target
Documentation=https://docs.mongodb.org/manual

[Service]
User=mongodb
Group=mongodb
ExecStart=/usr/bin/mongod --quiet --config /etc/mongod.conf

[Install]
WantedBy=multi-user.target
```

Llegados a este punto, MongoDB debería estar correctamente instalado, para arrancar o parar el servicio usaremos el comando `service`. Por ejemplo para reiniciar el servicio haremos:

```
$ sudo service mongod restart
```

Si ejecutamos el comando `mongo` podremos ver como se accede a la consola de MongoDB y se pueden ejecutar los diferentes comandos de MongoDB.

### Configuración de un replica set

#### Ubuntu 16.04 LTS

Para configurar la instalación de MongoDB como un nodo primario del replica set, debemos modificar el fichero `/etc/mongod.conf` para añadir las siguientes líneas.

```
replication:
  replSetName: "rs1"
```

Hecho esto, reiniciamos el servicio.

```
$ sudo service mongod restart
```

Ahora podemos acceder a la consola de MongoDB con el comando `mongo` para poder completar la configuración del replica set. Para ello, lo único que debemos hacer es inicializar el nodo como un replica set con el siguiente comando.

```
> rs.initiate()
```

Para comprobar que la configuración del replica set es correcta podemos ejecutar el comando `rs.conf()`. Cuando tengamos configurados otros nodos del replica set los podremos añadir con el comando `rs.add(...)`.

### PostgreSQL

El correcto funcionamiento de ToroDB Stampede depende de la existencia de un backend que permita el almacenamiento relacional, en esta versión el backend soportado es PostgreSQL.

#### Ubuntu 16.04 LTS

Se puede encontrar más información en el siguiente [enlace](https://www.digitalocean.com/community/tutorials/how-to-install-and-use-postgresql-on-ubuntu-16-04).

Instalación de los paquetes necesarios.

```
$ sudo apt-get update
$ sudo apt-get install postgresql postgresql-contrib
```

Podemos comprobar que la instalación ha sido satisfactoria accediendo a la consola de PostgreSQL.

```
$ sudo -u postgres psql
```

### Java

#### Ubuntu 16.04 LTS

La instalación de Oracle Java 8 en Ubuntu 16.04 se hace a partir de paquetes del sistema, por lo que basta ejecutar los siguientes comandos.

```
$ sudo add-apt-repository ppa:webupd8team/java
$ sudo apt-get update
$ sudo apt-get install oracle-java8-installer
```

### Maven

Aunque para ejecutar Toro Stampede no es necesario compilar el código fuente, si se desea hacerlo hay que instalar Maven 3 para poder obtener las dependencias y lanzar el proceso de construcción.

#### Ubuntu 16.04 LTS

Basta con descargar los paquetes oficiales, ejecutando los siguientes comandos.

```
$ sudo apt-get update
$ sudo apt-get install maven
```

## Instalación con docker

### Windows

### Linux

### Mac

## Instalación a partir  ficheros binarios

### Windows

### Linux

### Mac

Antes de arrancar ToroDB Stampede será necesario tener una instancia correctamente configurada de PostgreSQL y un MongoDB configurado como replica set.

[//]: # (Indicar dónde se puede descargar el fichero binario para MacOS)

```
$ tar xjvf <stampede-binary>.tar.bz2

$ torodb-stampede-<version>/bin/torodb-stampede
```

## Instalación a partir de los fuentes

### Windows

### Linux

### Mac

## Instalación con paquetes

### RPM

### Debian

## Configuración básica de ToroDB

### Config file

### CLI parameters

## Configuración avanzada

### Config file

### CLI parameters

### How to manage/parse Stampede logs

### Monitoring (metrics)
