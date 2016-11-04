One of the recommended ways to use ToroDB Stampede is through the binary distribution. It means that one precompiled distribution is downloaded and then executed using command tools.

# Linux/macOS

Given that [previous requirements](previous-requirements.md) are met, the only step needed to launch ToroDB Stampede is to download distribution from the next [link](http://todo) and execute it.

```no-highlight
$ wget http://todo

$ tar xjvf <stampede-binary>.tar.bz2

$ torodb-stampede-<version>/bin/torodb-stampede
```

The main problems at this step is that MongoDB or PostgreSQL has a different user/password than expected, to avoid that problem configuration files can be provided (among the use of `.toropass` file at the home path).

```no-highlight
$ torodb-stampede-<version>/bin/torodb-stampede --toropass-file <postgres-credentials-file> --mongopass-file <mongo-credentials-file>
```

## As a Linux systemd service

The steps described must be executed as root or using sudo command is Ubuntu is being used.

```no-highlight
$ ln -s "`pwd`/bin/torodb-stampede" /usr/bin/.

$ useradd -M -d "$(dirname "$(dirname "`pwd`/bin/torodb-stampede")")" torodb

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

### Accessing to logs

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

# Windows

TBD
