<h1>Installation From Source Code</h1>

Before starting, make sure the [prerequisites](prerequisites.md) are met.

## Linux/macOS

Download source code using Git:

```no-highlight
cd /tmp

git clone https://github.com/torodb/torodb.git
```

Compile source code and build the distribution using Apache Maven:

```no-highlight
cd torodb

mvn clean package -P assembler,prod
```
Create the [`.toropass` file](prerequisites.md#create-toropass-file) in the home directory (be sure the put the right password in):

```no-highlight
echo "localhost:5432:torod:torodb:<password>" > ~/.toropass
```

Extract and launch ToroDB Stampede (replace `$TOROHOME` with the desired ToroDB Stampede installation directory).

```no-highlight
cd "$TOROHOME"

tar xjf "$TOROHOME/stampede/main/target/dist/torodb-stampede-1.0.0-beta1.tar.bz2"

torodb-stampede-1.0.0-beta1/bin/torodb-stampede
```

## Windows

Download source code in a temporary directory:

```no-highlight
C:\tmp\>git clone https://github.com/torodb/torodb.git
```

Compile source code and build the distribution using Apache Maven:

```no-highlight
C:\tmp\>cd torodb

C:\tmp\torodb>mvn clean package -P assembler,prod
```

Create the [`.toropass` file](prerequisites.md#create-toropass-file) in the home directory (be sure the put the right password in):

```no-highlight
localhost:5432:torod:torodb:<password>
```

Decompress the Zip file in `C:\tmp\torodb\stampede\main\target\dist\torodb-stampede-1.0.0-beta1.zip` (replace `%TOROHOME%` with the desired ToroDB Stampede installation directory) and  execute the following command:

```no-highlight
C:\>%TOROHOME%\bin\torodb-stampede
```

Alternatively, double-click on the `torodb-stampede.bat` file in the `bin` folder.
