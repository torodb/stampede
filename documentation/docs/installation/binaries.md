<h1>Installation With Binaries</h1>

The fastest way to install ToroDB Stampede is to use the binary distribution.

Before starting, make sure the [prerequisites](prerequisites.md) are met.

## Linux/macOS

The script below executes the following steps:

1. Download the binary distribution [here](https://www.torodb.com/download/torodb-stampede-latest.tar.bz2).

2. Extract it

3. Start it using the [default configuration](prerequisites.md#postgresql-configuration) (assuming `.toropass` in the home directory)

```no-highlight
wget https://www.torodb.com/download/torodb-stampede-latest.tar.bz2

tar xjf torodb-stampede-*.tar.bz2

export TOROHOME="$(pwd)/torodb-stampede-*"

"$TOROHOME/bin/torodb-stampede"
```

### Configure as a Linux systemd Service

You can install ToroDB Stampede as a systemd service following the next steps:

```no-highlight
sudo ln -s "$TOROHOME/bin/torodb-stampede" /usr/bin/.

sudo useradd -M -d "$TOROHOME" torodb

sudo cp "$TOROHOME/systemd/torodb-stampede.service.sample" /lib/systed/system/torodb-stampede.service
```

#### Manage systemd service

##### Starting the service

Make shure you have enable ToroDB Stampede service. To enable the service just run:

```no-highlight
sudo systemctl enable torodb-stampede
```

To start the service run:

```no-highlight
sudo systemctl start torodb-stampede
```

##### Stopping the service

To stop ToroDB Stampede service:

```no-highlight
sudo systemctl stop torodb-stampede
```

##### Accessing the Logs

To view logs of ToroDB Stampede service:

```no-highlight
sudo journalctl --no-pager -u torodb-stampede
```

Following logs:

```no-highlight
sudo journalctl --no-pager -u torodb-stampede -f
```

View all logs:

```no-highlight
sudo journalctl --no-tail --no-pager -u torodb-stampede
```


## Windows

Follow these steps to download and start ToroDB Stampede using the [default configuration](prerequisites.md#postgresql-configuration) (assuming `.toropass` in the home directory):

1. Download the binary distribution from [here](https://www.torodb.com/download/torodb-stampede-latest.zip)
2. Decompress the downloaded Zip file in the desired ToroDB Stampede directory (`%TOROHOME%`)
3. Execute the command `C:\>%TOROHOME%\bin\torodb-stampede` or double-click on the `torodb-stampede.bat` file in folder `bin`



