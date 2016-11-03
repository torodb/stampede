ToroDB Stampede can be tested in a Docker container in two different ways. First option is to download the containers from a repository, the second one is to launch with Maven Docker tasks.

# From Docker repository

## With docker image

```
$ docker run -ti -v `realpath <postgres-credentials-file>`:/root/.toropass 8kdata/torodb-stampede
```

or

```
$ TORODB_BACKEND_PASSWORD="<torodb user's password>"

$ docker run -ti 8kdata/torodb-stampede
```

## With docker-compose

Download the docker compose file and run it:

```
$ wget http://todo

$ docker-compose up
```

# From source code

## Linux/macOS

The source code contains some Maven tasks that can build the right artifacts to execute ToroDB Stampede and its dependencies in Docker containers. It can be done with the next commands.

```
$ mvn clean package -P docker -Ddocker.skipbase=false

$ mvn -f packaging/stampede/main/pom.xml -P docker-stampede-fullstack docker:run -Ddocker.follow
```

Some errors can appear due to the Docker cache, if that happens then cache can be disabled using command options, like is done in the next example. Usually these errors are related to network connection timeouts.

```
$  mvn clean package -P docker -Ddocker.skipbase=false -Ddocker.nocache=true
```
