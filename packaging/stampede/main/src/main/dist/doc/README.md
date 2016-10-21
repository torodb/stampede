# @{assembler.fullName}

This is the @{assembler.fullName} binary distribution. It include a script 

## Installation

Please follow these instructions to install @{assembler.fullName} on your system.

### System configuration

@{assembler.fullName} work with PostgreSQL version >=9.4. Depending on the system this 
guide will help you install minimal version from upstream or from postgresql
official repositories. If you want to have latest PostgreSQL version 
please refer to 
[official PostgreSQL installation guide|https://www.postgresql.org/download/]  

#### Debian/Ubuntu

Install OpenJDK 8 JRE headless and PostgreSQL 9.6/9.5 client and server:

* For Debian 8:

Add backports to /etc/apt/sources.list:

    version=$(cat /etc/os-release|grep VERSION=|cut -d '(' -f 2|cut -d ')' -f 1)
    echo "deb http://http.debian.net/debian ${version}-backports main" >> /etc/apt/sources.list

Update repositories:

    apt-get update

Install openjdk 8 jre headless and postgresql 9.4: 

    apt-get install openjdk-8-jre-headless postgresql

* For Ubuntu 16.04:

Install openjdk 8 jre headless and postgresql 9.5: 

    sudo apt-get install openjdk-8-jre-headless postgresql

Start PostgreSQL:

* For Debian 7 and 8:

    service postgresql start

* For Ubuntu 16.04:
    
    sudo service postgresql start

#### CentOS/Fedora

First install postgresql 9.6 repository:


* For CentOS/RHEL 7

    rpm -Uvh http://yum.postgresql.org/9.6/redhat/rhel-6-x86_64/pgdg-redhat96-9.6-3.noarch.rpm

* For CentOS/RHEL 6

    rpm -Uvh http://yum.postgresql.org/9.6/redhat/rhel-7-x86_64/pgdg-redhat96-9.6-3.noarch.rpm

* For CentOS/RHEL 5

    rpm -Uvh http://yum.postgresql.org/9.6/redhat/rhel-5-x86_64/pgdg-redhat96-9.6-3.noarch.rpm

* For Fedora 23

    rpm -Uvh http://yum.postgresql.org/9.6/fedora/fedora-23-x86_64/pgdg-fedora96-9.6-3.noarch.rpm

* For Fedora 22

    rpm -Uvh http://yum.postgresql.org/9.6/fedora/fedora-22-x86_64/pgdg-fedora96-9.6-3.noarch.rpm

* For Fedora 21

    rpm -Uvh http://yum.postgresql.org/9.6/fedora/fedora-21-x86_64/pgdg-fedora96-9.6-3.noarch.rpm


Install OpenJDK 8 JRE headless and PostgreSQL 9.6 client and server:

    yum install java-1.8.0-openjdk-headless postgresql96 postgresql96-server
    
Setup PostgreSQL:

    /usr/pgsql-9.6/bin/postgresql96-setup initdb

Start PostgreSQL:

* For CentOS/RHEL 7 and Fedora 23

    systemctl start postgresql-9.6
    systemctl enable postgresql-9.6

* For CentOS/RHEL 6/5 and Fedora 22/21

    service  postgresql-9.6 start
    chkconfig postgresql-9.6 on

#### Mac OS X

Download Java JDK 8 from [Oracle|http://www.oracle.com/technetwork/java/javase/downloads/index.html]

* Select JDK Download button
* Select Mac OS X with .dmg file option
* Open dmg file and follow installation process

Download PostgreSQL 9.6 from [EnterpriseDB|http://www.enterprisedb.com/products-services-training/pgdownload]:

* Select Mac OS X option
* Open dmg file and follow installation process

Now create a symbolic link for PostgreSQL client `psql` in `/usr/local/bin` location so
it will be possible to connect to PostgreSQL without issuing the entire path to `psql`:

    sudo mkdir -p /usr/local/bin
    sudo ln -s /Library/PostgreSQL/9.6/bin/psql /usr/local/bin/psql

To allow setup script to work without prompting postgres user password each time it connect to PostgreSQL
it is convenient to create file `/var/root/.pgpass`:

    sudo echo "localhost:5432:*:postgres:$(read -p "Type postgres user's password:"$'\n' -s pwd; echo $pwd)" | sudo tee /var/root/.pgpass > /dev/null
    sudo chmod 400 /var/root/.pgpass

#### Windows

Download Java JDK 8 from [Oracle|http://www.oracle.com/technetwork/java/javase/downloads/index.html]

* Select JRE Download button
* Select Windows Offline with .exe file option
* Open exe file and follow installation process

Download PostgreSQL 9.6 from [EnterpriseDB|http://www.enterprisedb.com/products-services-training/pgdownload]:

* Select Win option
* Open exe file and follow installation process

Now add PostgreSQL client `psql` to the `PATH` environment variable so
it will be possible to connect to PostgreSQL without issuing the entire path to `psql`. 
Open the Windows command prompt and type:

    set PATH=%PATH%;C:/Program Files/PostgreSQL/9.6/bin/
    setx PATH "%PATH%"

To allow setup script to work without prompting postgres user password each time it connect to PostgreSQL
it is convenient to create file `%APPDATA%\postgresql\pgpass.conf`:

    mkdir "%APPDATA%\postgresql"
    set /p pwd="Type postgres user's password:" & cls
    echo localhost:5432:*:postgres:%pwd%> "%APPDATA%\postgresql\pgpass.conf"
    attrib +I +A "%APPDATA%\postgresql\pgpass.conf"

### Configuration

Configuration file used by `bin/@{assembler.name}` is `conf/@{assembler.name}.conf`. 
You can change it or overwrite it using parameters. 

Please run `@{assembler.name} -h` or refer to [online documentation|@{assembler.url}] 
for more info about @{assembler.fullName} configuration and parameters.

### System setup

Run setup script as root (prepend by `sudo` on Ubuntu) passing as parameter the name of the 
system user that will run @{assembler.fullName} to create PostgreSQL user and database:

    bin/@{assembler.name}-setup <user name> 

* For windows (will use current user to create PostgreSQL user and database):

    bin\@{assembler.name}-setup.bat

## Run @{assembler.fullName}

To run @{assembler.fullName}:

    bin/@{assembler.name}

* For windows:

    bin\@{assembler.name}.bat

