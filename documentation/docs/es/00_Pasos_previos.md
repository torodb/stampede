[TOC]

## Instalación de MongoDB Community Edition

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

## Configuración de un replica set en MongoDB

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

## Instalación de PostgreSQL
