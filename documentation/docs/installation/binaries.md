<h1>Installation with binaries</h1>

One of the recommended ways to use ToroDB Stampede is through the binary distribution. It means that a precompiled distribution is downloaded and then executed using command tools.

## Linux/macOS

Given that [previous requirements](previous-requirements.md) are met and default configuration is used, to launch ToroDB Stampede download distribution from the next [link](https://www.torodb.com/download/torodb-stampede-1.0.0-beta2.tar.bz2), extract and execute it.

```no-highlight
wget https://www.torodb.com/download/torodb-stampede-1.0.0-beta2.tar.bz2

tar xjf torodb-stampede-1.0.0-beta2.tar.bz2

export TOROHOME="$(pwd)/torodb-stampede-1.0.0-beta2"

"$TOROHOME/bin/torodb-stampede"
```

### Configure as a Linux systemd service

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

##### Accessing logs

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

Given that [previous requirements](previous-requirements.md#create-toropass-file) are met, the only step needed to launch ToroDB Stampede is:

* Download distribution from the next [link](https://www.torodb.com/download/torodb-stampede-1.0.0-beta2.zip).
* Uncompress the downloaded Zip file in the final ToroDB Stampede directory (`%TOROHOME%`).
* Execute the command `C:\>%TOROHOME%\bin\torodb-stampede` or simply, double click on the `torodb-stampede.bat` file located in folder `bin`.



