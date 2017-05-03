<h1>Installation with Docker</h1>
ToroDB Stampede can be tested in a Docker container in two different ways. First option is to download the container (or containers if you chose to use Docker Compose) from a repository, the second one is to launch with Maven Docker tasks (if source code was previously downloaded).

## From Docker repository

### With Docker image

If `.toropass` file is created the docker containers can be launched with the command below.

```no-highlight
docker run -ti -v `realpath <postgres-credentials-file>`:/root/.toropass torodb/stampede
```

In other case it will be enough with the creation of the environment variable `TORODB_BACKEND_PASSWORD`.

```no-highlight
TORODB_BACKEND_PASSWORD="<password>"

docker run -ti torodb/stampede
```

### With Docker Compose

The docker compose file must be downloaded and executed.

```no-highlight
wget https://raw.githubusercontent.com/torodb/stampede/master/main/src/main/dist/docker/compose/torodb-stampede-fullstack/docker-compose.yml

docker-compose up
```

## From source code

### Linux/macOS

The source code contains some Maven tasks that can build the right artifacts to execute ToroDB Stampede and its dependencies in Docker containers.

```no-highlight
mvn clean package -P prod,docker -Ddocker.skipbase=false

mvn -f stampede/main/pom.xml -P docker-stampede-fullstack docker:run -Ddocker.follow
```

Sometimes, errors can appear due to the Docker cache. If that happens, cache can be disabled using command options, like is done in the next example. Usually these errors are related to network connection timeouts.

```no-highlight
mvn clean package -P prod,docker -Ddocker.skipbase=false -Ddocker.nocache=true
```
