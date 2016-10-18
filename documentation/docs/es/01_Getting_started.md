__Nota__: La siguiente documentación entiende que se ha realizado una correcta instalación de MongoDB como replica set y de PostgreSQL. Si no se han completado estos pasos, se explica como llevarlos a acabo en el apartado [Pasos previos](00_Pasos_previos.md).

[TOC]

## Configuración del backend

Para poder hacer la replicación de forma correcta, Toro Stampede necesita un backend correctamnte configurado. Es decir, una base de datos que haga de motor de persistencia de los datos que se desean replicar.

### PostgreSQL

Suponiendo que tenemos una instalación correcta de PostgreSQL, hay que realizar dos tareas.
* Crear el rol `torodb` con permisos para crear bases de datos y hacer login
* Crear la base de datos `torod` en PostgreSQL, que se usará para almacenar la meta-información de Toro Stampede, cuyo owner es `torodb`.

```
$ sudo -u postgres createuser --interactive

$ sudo -u postgres createdb -O torodb torod
```

## Arranque de Toro Stampede
