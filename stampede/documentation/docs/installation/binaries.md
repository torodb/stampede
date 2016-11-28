<h1>Installation with binaries</h1>

One of the recommended ways to use ToroDB Stampede is through the binary distribution. It means that a precompiled distribution is downloaded and then executed using command tools.

## Linux/macOS

Given that [previous requirements](previous-requirements.md) are met and default configuration is used, the only step needed to launch ToroDB Stampede is to download distribution from the next [link](http://todo) and execute it.

```no-highlight
$ cd $TOROHOME

$ wget http://todo

$ tar xjf torodb-stampede-<version>.tar.bz2

$ torodb-stampede-<version>/bin/torodb-stampede
```

### Configure as a Linux systemd service

You can install ToroDB Stampede as a systemd service following the next steps. This must be executed as root or using sudo command if Ubuntu is being used.

```no-highlight
$ ln -s "$TOROHOME/bin/torodb-stampede" /usr/bin/.

$ useradd -M -d "$TOROHOME" torodb

$ cp systemd/torodb-stampede.service.sample /lib/systed/system/.
```

Enable and start the newly created ToroDB Stampede service:

```no-highlight
$ systemctl enable torodb-stampede

$ systemctl start torodb-stampede
```

To stop ToroDB Stampede service:

```no-highlight
$ systemctl stop torodb-stampede
```

#### Accessing to logs

To view logs of ToroDB Stampede service:

```no-highlight
$ journalctl --no-pager -u torodb-stampede
```

Following logs:

```no-highlight
$ journalctl --no-pager -u torodb-stampede -f
```

View all logs:

```no-highlight
$ journalctl --no-tail --no-pager -u torodb-stampede
```


## Windows

Given that [previous requirements](previous-requirements.md#create-toropass-file) are met, the only step needed to launch ToroDB Stampede is:

* Download distribution from the next [link](http://todo-zip).
* Uncompress the downloaded Zip file in the final ToroDB Stampede directory (`%TOROHOME%`).
* Execute the command `C:\>%TOROHOME%\bin\torodb-stampede` or simply, double click on the `torodb-stampede.bat` file located in folder `bin`.



