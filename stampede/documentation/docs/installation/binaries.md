<h1>Installation With Binaries</h1>

The fastest way to install ToroDB Stampede is to use the binary distribution.

Before starting, make sure the [prerequisites](prerequisites.md) are met.

## Linux/macOS

The script below executes the following steps:

1. Download the binary distribution [here](https://www.torodb.com/download/torodb-stampede-1.0.0-beta1.tar.bz2).  

2. Extract it 

3. Start it using the [default configuration](prerequisites.md#postgresql-configuration) (assuming `.toropass` in the home directory)

```no-highlight
wget https://www.torodb.com/download/torodb-stampede-1.0.0-beta1.tar.bz2

tar xjf torodb-stampede-1.0.0-beta1.tar.bz2

export TOROHOME="$(pwd)/torodb-stampede-1.0.0-beta1"

"$TOROHOME/bin/torodb-stampede"
```

### Configure as a Linux systemd Service

To run ToroDB Stampede as a systemd service on Ubuntu, the following script must be executed as root:

```no-highlight
ln -s "$TOROHOME/bin/torodb-stampede" /usr/bin/.

useradd -M -d "$TOROHOME" torodb

cp "$TOROHOME/systemd/torodb-stampede.service.sample" /lib/systed/system/torodb-stampede.service
```

Enable and start the newly created ToroDB Stampede service:

```no-highlight
systemctl enable torodb-stampede

systemctl start torodb-stampede
```

To stop ToroDB Stampede service:

```no-highlight
systemctl stop torodb-stampede
```

#### Accessing the Logs

To view logs of ToroDB Stampede service:

```no-highlight
journalctl --no-pager -u torodb-stampede
```

Following logs:

```no-highlight
journalctl --no-pager -u torodb-stampede -f
```

View all logs:

```no-highlight
journalctl --no-tail --no-pager -u torodb-stampede
```


## Windows

Follow these steps to download and start ToroDB Stampede using the [default configuration](prerequisites.md#postgresql-configuration) (assuming `.toropass` in the home directory):

1. Download the binary distribution from [here](https://www.torodb.com/download/torodb-stampede-1.0.0-beta1.zip)
2. Decompress the downloaded Zip file in the desired ToroDB Stampede directory (`%TOROHOME%`)
3. Execute the command `C:\>%TOROHOME%\bin\torodb-stampede` or double-click on the `torodb-stampede.bat` file in folder `bin`



