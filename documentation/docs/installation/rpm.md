<h1>Installation For Fedora/CentOS</h1>
ToroDB Stampede can be installed from a COPR repository in two flavours:

* **torodb-stampede**
  This package contains ToroDB Stampede and has no PostgreSQL dependency. Use this if you are running PostgreSQL on a different machine.
* **torodb-stampede-postgres**  
  This package contains ToroDB Stampede and has a PostreSQL dependency. Use this for minimal configuration effort if you are running ToroDB Stampede and PostgreSQL on the same machine.

## Install package torodb-stampede

Just run as root user:

### For Fedora 21 / CentOS

```no-highlight
yum -y install yum-plugin-copr
yum -y copr enable eightkdata/torodb
yum -y install torodb-stampede
```

### For Fedora >= 22

```no-highlight
dnf -y install dnf-plugins-core
dnf -y copr enable eightkdata/torodb
dnf -y install torodb-stampede
```

And then to setup ToroDB Stampede run interactive script as root user:

```no-highlight
torodb-stampede-setup
```

You will be prompted to provide superuser credentials (if you didn't created ToroDB's database and user yourself), ToroDB's user credentials and MongoDB credentials.

!!! info "Manage ToroDB Stampede service"
    To manage ToroDB Stampede service please refer to [manage systemd service section](binaries#manage-systemd-service). 

## Install package torodb-stampede-postgres

Just run as root user:

### For Fedora 21 / CentOS

```no-highlight
yum -y install yum-plugin-copr
yum -y copr enable eightkdata/torodb
yum -y install torodb-stampede-postgres
```

### For Fedora >= 22

```no-highlight
dnf -y install dnf-plugins-core
dnf -y copr enable eightkdata/torodb
dnf -y install torodb-stampede-postgres
```

And then to setup ToroDB Stampede run interactive script as root user:

```no-highlight
torodb-stampede-setup
```

You will be prompted to provide MongoDB credentials.

!!! info "Manage ToroDB Stampede service"
    To manage ToroDB Stampede service please refer to [manage systemd service section](binaries#manage-systemd-service). 

## Nightly build packages

To install latest unstable nightly build packages just use the torodb-dev repository (as root):

### For Fedora 21 / CentOS

```no-highlight
yum -y install yum-plugin-copr
yum -y copr enable eightkdata/torodb-dev
yum -y install torodb-stampede
```

### For Fedora >= 22

```no-highlight
dnf -y install dnf-plugins-core
dnf -y copr enable eightkdata/torodb-dev
dnf -y install torodb-stampede
```
