The installation from the source code is quite similar to the binary installation, but it is necessary to build ToroDB Stampede from the sources first.

# Linux/macOS

Download source code.

[ToDo]: <> (Update the repository address)

```no-highlight
$ git clone https://ydarias@bitbucket.org/8kdata/torodb-poc.git
```

Compile source code.

```no-highlight
$ cd torodb-poc

$ mvn clean package -P assembler
```

Create `.toropass` file at current user home directory with the next content.

```no-highlight
localhost:5432:torod:torodb:<password>
```

Launch ToroDB Stampede.

```no-highlight
$ cp packaging/stampede/main/target/dist/torodb-stampede-<version>.tar.bz2 <test-dir>/torodb-stampede-<version>.tar.bz2

$ cd <test-dir>

$ tar xjvf torodb-stampede-<version>.tar.bz2

$ torodb-stampede-<version>/bin/torodb-stampede
```

# Windows
