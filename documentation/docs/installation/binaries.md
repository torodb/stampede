One recommended way to use ToroDB Stampede is through the binary distribution. It means that one precompiled distribution is downloaded and then executed using command tools.

# Linux/macOS

Given that previous prerequisites are met, the only step needed to launch ToroDB Stampede is the download of the binary distribution from the next [link](http://todo).

```
$ wget http://todo

$ tar xjvf <stampede-binary>.tar.bz2

$ torodb-stampede-<version>/bin/torodb-stampede
```

The main problems at this step is that MongoDB or PostgreSQL has a different user/password than expected, to avoid that problem configuration files can be provided.

```
$ torodb-stampede-<version>/bin/torodb-stampede --toropass-file <postgres-credentials-file> --mongopass-file <mongo-credentials-file>
```

## As a Linux systemd service

All the following commands must be run as root. Precede them by sudo command if you are running in Ubuntu.

First you have to create a symbolic link to `bin/torodb-stampede` in `/usr/bin` folder:

```
$ ln -s "`pwd`/bin/torodb-stampede" /usr/bin/.
```

You have to create the system user `torodb`:

```
$ useradd -M -d "$(dirname "$(dirname "`pwd`/bin/torodb-stampede")")" torodb
```

Now copy the file `systemd/torodb-stampede.service.sample` to `/lib/systed/system` folder:

```
$ cp systemd/torodb-stampede.service.sample /lib/systed/system/.
```

Enable and start the newly created ToroDB Stampede service:

```
$ systemctl enable torodb-stampede

$ systemctl start torodb-stampede
```

To view logs of ToroDB Stampede service:

```
$ journalctl --no-pager -u torodb-stampede
```

Following logs:

```
$ journalctl --no-pager -u torodb-stampede -f
```

View all logs:

```
$ journalctl --no-tail --no-pager -u torodb-stampede
```

To stop ToroDB Stampede service:

```
$ systemctl stop torodb-stampede
```

# Windows

TBD
