<h1>Installation from source code</h1>

The installation from the source code is quite similar to the binary installation, but it is necessary to build ToroDB Stampede from the sources first.

Among the dependencies you found in [previous requirements](previous-requirements.md#project-dependencies) section, if we want to compile the source code other requisites are mandatory.

| | Description | External links |
|-|-------------|----------------|
| Git | It is the distributed version control system (DVCS) used to keep ToroDB Stampede source code up to date and synchronized between its committers. | [more info](https://git-scm.com/downloads) |
| Maven | Dependency management and construction tasks has been delegated to Apache Maven, so it is necessary to compile the source code. | [more info](http://maven.apache.org/install.html) | 
| Docker | An open-source project that automates the deployment of Linux applications inside software containers. It allow to run a ToroDB Stampede and to test it in a controlled environment. | [more info](https://docs.docker.com/) | 
| Docker Compose | A tool for defining and running multi-container Docker applications. It allow to run test scenarios like a ToroDB Stampede replicating from a MongoDB and connected to a PostgreSQL. | [more info](https://docs.docker.com/compose/install/) | 

## Linux/macOS

Download source code.

```no-highlight
cd /tmp

git clone https://github.com/torodb/stampede.git
```

Compile source code.

```no-highlight
cd stampede

mvn clean package -P assembler,prod
```

As explained in [previous requirements](previous-requirements.md#create-toropass-file) section, create `.toropass` file at current user home directory with the next content.

```no-highlight
echo "localhost:5432:torod:torodb:<password>" > ~/.toropass
```

Extract and launch ToroDB Stampede (replace `$TOROHOME` with final ToroDB Stampede directory).

```no-highlight
cd "$TOROHOME"

tar xjf "$TOROHOME/stampede/main/target/dist/torodb-stampede-1.0.0-beta2.tar.bz2"

torodb-stampede-1.0.0-beta2/bin/torodb-stampede
```

## Windows

Download source code in some temporal directory.

```no-highlight
C:\tmp\>git clone https://github.com/torodb/stampede.git
```

Compile source code.

```no-highlight
C:\tmp\>cd stampede

C:\tmp\stampede>mvn clean package -P assembler,prod
```

As explained in [previous requirements](previous-requirements.md#create-toropass-file) section, create `.toropass` file at current user home directory `%HOME%\.toropass` with the next content.

```no-highlight
localhost:5432:torod:torodb:<password>
```

Uncompress the Zip file located in `C:\tmp\torodb\stampede\main\target\dist\torodb-stampede-1.0.0-beta2.zip` in the final ToroDB Stampede directory (replace `%TOROHOME%` with final ToroDB Stampede directory), and then execute the command:

```no-highlight
C:\>%TOROHOME%\bin\torodb-stampede
```

or simply, double click on the `torodb-stampede.bat` file located in folder `bin`.
