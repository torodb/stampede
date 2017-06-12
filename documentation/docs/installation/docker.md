<h1>Installation With Docker</h1>

To run ToroDB Stampede in a Docker container, you can either download the container(s) from the Docker repository or build it from the ToroDB sources.

## Installation From The Docker Repository

### With Docker Image

If `.toropass` file is created the docker containers can be launched with the following command:

```no-highlight
docker run -ti -v `realpath <postgres-credentials-file>`:/root/.toropass torodb/stampede
```

If you configured the PostgreSQL database with ToroDB Stampede default's, you can just provide the password in an environment variable:

```no-highlight
TORODB_BACKEND_PASSWORD="<password>"

docker run -ti torodb/stampede
```

### With Docker Compose

Download and execute the Docker Compose file:

```no-highlight
wget https://raw.githubusercontent.com/torodb/stampede/master/main/src/main/dist/docker/compose/torodb-stampede-fullstack/docker-compose.yml

docker-compose up
```

## From Source Code

### Linux/macOS

To get the ToroDB Stampede source code, clone it via Git:

```no-highlight
git clone https://github.com/torodb/torodb.git
```

The Maven configuration provides tasks to build Docker containers for ToroDB Stampede and its dependencies.

```no-highlight
mvn clean package -P prod,docker -Ddocker.skipbase=false

mvn -f stampede/main/pom.xml -P docker-stampede-fullstack docker:run -Ddocker.follow
```
If network (or other) errors occur, it often helps to disable the Docker cache as shown below.

```no-highlight
mvn clean package -P prod,docker -Ddocker.skipbase=false -Ddocker.nocache=true
```
