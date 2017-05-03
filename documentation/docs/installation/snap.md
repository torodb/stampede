<h1>Installation with SNAP</h1>
ToroDB Stampede can be installed from SNAP public store in two flavours:

* torodb-stampede: in this package ToroDB Stampede comes alone without the backend included so you will have to provide one.
* torodb-stampede-postgres: in this package ToroDB Stampede comes with a PostgreSQL so you will be able to start using it with minimal configuration.

## Install package torodb-stampede

Just run:

```
sudo snap install torodb-stampede
```

And then to setup ToroDB Stampede run interactive script as root user:

```
sudo torodb-stampede.setup
```

You will be prompted to provide superuser credentials (if you didn't created ToroDB's database and user yourself), ToroDB's user credentials and MongoDB credentials.

!!! info "Manage ToroDB Stampede service"
    To manage ToroDB Stampede service please refer to [manage systemd service section](binaries#manage-systemd-service). 
    Replace service name from *torodb-stampede* to *snap.torodb-stampede.daemon.service*.

## Install package torodb-stampede-postgres

Just run:

```
sudo snap install torodb-stampede-postgres
```

And then to setup ToroDB Stampede run interactive script as root user:

```
sudo torodb-stampede.setup
```

You will be prompted to provide MongoDB credentials.

!!! info "Manage ToroDB Stampede service"
    To manage ToroDB Stampede service please refer to [manage systemd service section](binaries#manage-systemd-service). 
    Replace service name from *torodb-stampede* to *snap.torodb-stampede-postgres.daemon.service*.

## Nightly build packages

To install latest unstable nightly build packages just add `--edge` and `--devmode` parameter to `snap` command:

```
sudo snap install torodb-stampede --edge --devmode
```
