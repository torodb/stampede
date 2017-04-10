<h1>Installation for Ubuntu/Debian</h1>
ToroDB Stampede can be installed from a PPA repository in two flavours:

* torodb-stampede: in this package ToroDB Stampede comes without the backend dependency. This package is used when you have PostgreSQL installed in a different machine.
* torodb-stampede-postgres: in this package ToroDB Stampede comes with a PostgreSQL dependency. This package is handy if you want to minimize configuration steps but have te requirements of install ToroDB Stamepde and PostgreSQL server in separate machines.

## Install package torodb-stampede

Just run:

```
sudo add-apt-repository -y ppa:8kdata
sudo apt update
sudo apt install torodb-stampede
```

And then to setup ToroDB Stampede run interactive script as root user:

```
sudo torodb-stampede-setup
```

You will be prompted to provide superuser credentials (if you didn't created ToroDB's database and user yourself), ToroDB's user credentials and MongoDB credentials.

!!! info "Manage ToroDB Stampede service"
    To manage ToroDB Stampede service please refer to [manage systemd service section](binaries#manage-systemd-service)

## Install package torodb-stampede-postgres

Just run:

```
sudo add-apt-repository -y ppa:8kdata
sudo apt update
sudo apt install torodb-stampede-postgres
```

And then to setup ToroDB Stampede run interactive script as root user:

```
sudo torodb-stampede-setup
```

You will be prompted to provide MongoDB credentials.

!!! info "Manage ToroDB Stampede service"
    To manage ToroDB Stampede service please refer to [manage systemd service section](binaries#manage-systemd-service). 

## Nightly build packages

To install latest unstable nightly build packages just use ppa-dev repository:

```
sudo add-apt-repository -y ppa:8kdata/ppa-dev
sudo apt update
sudo apt install torodb-stampede
```
