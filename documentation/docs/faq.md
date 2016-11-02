## The command wget is not found in macOS

By default macOS hasn't the wget tool in the terminal, if you want to use it [Homebrew](http://brew.sh) can be used.

Once installed Homebrew, it can be installed with `brew install wget`.

## FATAL: no pg_hba.conf entry for host "...", user "...", database "...", SSL off

Some installations of PostgreSQL are configured with strict security configuration. This make PostgreSQL reject host connection 
through TCP connections. In this case you may have to change `pg_hba.conf` file (usually located in the PostgreSQL's data 
directory or configuration directory) with rule to allow access to the database for the user specified in ToroDB Stampede 
configuration from the ToroDB Stampede's host. For example to give access from localhost to database torod for user torodb add
following lines to `pg_hba.conf` file:

    host    torod   torodb      127.0.0.1/32    md5
    host    torod   torodb      ::1/128    md5

Please make sure that those new rules must precede any other host rule for same host (eg: 127.0.0.1/32) that apply to all users. For more
informations on `pg_hba.conf` refer to the [Official PostgreSQL documentation|https://www.postgresql.org/docs/current/static/auth-pg-hba-conf.html].