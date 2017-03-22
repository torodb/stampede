<h1>Installation for Fedora/CentOS</h1>
ToroDB Stampede can be installed from a COPR repository in two flavours:

* torodb-stampede: in this package ToroDB Stampede comes without the backend dependency. This package is used when you have PostgreSQL installed in a different machine.
* torodb-stampede-postgres: in this package ToroDB Stampede comes with a PostgreSQL dependency. This package is handy if you want to minimize configuration steps but have te requirements of install ToroDB Stamepde and PostgreSQL server in separate machines.

## Install package torodb-stampede

Just run:

### For Fedora 21 / CentOS

```
yum -y install yum-plugin-copr
yum -y copr enable 8kdata/torodb
yum -y install torodb-stampede
```

### For Fedora >= 22
```
dnf -y install dnf-plugins-core
dnf -y copr enable 8kdata/torodb
dnf -y install torodb-stampede
```

And then to setup ToroDB Stampede run interactive script as root user:

```
sudo torodb-stampede.setup
```

You will be prompted to provide superuser credentials (if you didn't created ToroDB's database and user yourself), ToroDB's user credentials and MongoDB credentials.

## Install package torodb-stampede-postgres

Just run:

### For Fedora 21 / CentOS

```
yum -y install yum-plugin-copr
yum -y copr enable 8kdata/torodb
yum -y install torodb-stampede-postgres
```

### For Fedora >= 22
```
dnf -y install dnf-plugins-core
dnf -y copr enable 8kdata/torodb
dnf -y install torodb-stampede-postgres
```

And then to setup ToroDB Stampede run interactive script as root user:

```
sudo torodb-stampede.setup
```

You will be prompted to provide MongoDB credentials.

## Nightly build packages

To install latest unstable nightly build packages just use torodb-dev repository:


### For Fedora 21 / CentOS

```
yum -y install yum-plugin-copr
yum -y copr enable 8kdata/torodb-dev
yum -y install torodb-stampede
```

### For Fedora >= 22
```
dnf -y install dnf-plugins-core
dnf -y copr enable 8kdata/torodb-dev
dnf -y install torodb-stampede
```
