<h1>Installation from source code</h1>

The installation from the source code is quite similar to the binary installation, but it is necessary to build ToroDB Stampede from the sources first.

## Linux/macOS

Download source code.

[ToDo]: <> (Update the repository address)

```no-highlight
$ git clone https://github.com/torodb/torodb.git
```

Compile source code.

```no-highlight
$ cd torodb

$ mvn clean package -P assembler,prod
```

As explained in [previous requirements](previous-requirements.md#create-toropass-file) section, create `.toropass` file at current user home directory with the next content.

```no-highlight
localhost:5432:torod:torodb:<password>
```

Launch ToroDB Stampede.

```no-highlight
$ cp packaging/stampede/main/target/dist/torodb-stampede-<version>.tar.bz2 $TOROHOME/torodb-stampede-<version>.tar.bz2

$ cd $TOROHOME

$ tar xjf torodb-stampede-<version>.tar.bz2

$ torodb-stampede-<version>/bin/torodb-stampede
```

## Windows

Download source code in some temporal directory.

```no-highlight
C:\tmp\>git clone https://github.com/torodb/torodb.git
```

Compile source code.

```no-highlight
C:\tmp\>cd torodb

C:\tmp\torodb>mvn clean package -P assembler,prod
```

As explained in [previous requirements](previous-requirements.md#create-toropass-file) section, create `.toropass` file at current user home directory `C:\Users\<YOUR USER NAME>\.toropass` with the next content.

```no-highlight
localhost:5432:torod:torodb:<password>
```

Uncompress the Zip file located in `C:\tmp\torodb\packaging\stampede\main\target\dist\torodb-stampede-<version>-SNAPSHOT.zip` in the final ToroDB Stampede directory, and then execute the command:

```no-highlight
C:\>%TOROHOME%\bin\torodb-stampede
```

or simply, double click on the `torodb-stampede.bat` file located in folder `bin`.
