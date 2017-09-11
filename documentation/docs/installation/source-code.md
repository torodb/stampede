<h1>Installation From Source Code</h1>

## Prerequisites


In addition to the runtime dependencies (see [runtime dependencies](prerequisites.md#project-dependencies)), the following components are required to build ToroDB Stampeded from the sources.

| | Description | External links |
|-|-------------|----------------|
| Git | It is the distributed version control system (DVCS) used to mange ToroDB Stampede source code. | [more info](https://git-scm.com/downloads) |
| Apache Maven | The build tool used by ToroDB Stampede. | [more info](http://maven.apache.org/install.html) |
| Docker | A container management tool for Linux. It is used to test ToroDB Stampede. | [more info](https://docs.docker.com/) |
| Docker Compose | A tool for defining and running multi-container Docker applications. It allow to run test scenarios like a ToroDB Stampede replicating from a MongoDB and connected to a PostgreSQL. | [more info](https://docs.docker.com/compose/install/) |

## Linux/macOS

Download source code using Git:

```no-highlight
cd /tmp

git clone https://github.com/torodb/stampede.git
```

Compile source code and build the distribution using Apache Maven:

```no-highlight
cd stampede

mvn clean package -P assembler,prod
```
Create the [`.toropass` file](prerequisites.md#create-toropass-file) in the home directory (be sure the put the right password in):

```no-highlight
echo "localhost:5432:torod:torodb:<password>" > ~/.toropass
```

Extract and launch ToroDB Stampede (replace `$TOROHOME` with the desired ToroDB Stampede installation directory).

```no-highlight
cd "$TOROHOME"

tar xjf "$TOROHOME/stampede/main/target/dist/torodb-stampede-*.tar.bz2"

torodb-stampede-*/bin/torodb-stampede
```

## Windows

Download source code in a temporary directory:

```no-highlight
C:\tmp\>git clone https://github.com/torodb/stampede.git
```

Compile source code and build the distribution using Apache Maven:

```no-highlight
C:\tmp\>cd stampede

C:\tmp\stampede>mvn clean package -P assembler,prod
```

Create the [`.toropass` file](prerequisites.md#create-toropass-file) in the home directory (be sure the put the right password in):

```no-highlight
localhost:5432:torod:torodb:<password>
```

Decompress the Zip file in `C:\tmp\torodb\stampede\main\target\dist\torodb-stampede-<version>.zip` (replace `%TOROHOME%` with the desired ToroDB Stampede installation directory) and  execute the following command:

```no-highlight
C:\>%TOROHOME%\bin\torodb-stampede
```

Alternatively, double-click on the `torodb-stampede.bat` file in the `bin` folder.
