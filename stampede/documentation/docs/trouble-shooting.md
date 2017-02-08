<h1>Trouble Shooting</h1>

## No pg_hba.conf entry

Depending on the Linux distribution and PostgreSQL installation, the error below could appear.

```
FATAL: no pg_hba.conf entry for host "...", user "...", database "...", SSL off
```

This happens if PostgreSQL is configured with strict security policies and thus rejects connections through TCP. The `pg_hba.conf` file (usually in the PostgreSQL's data directory or configuration directory) must be edited with a rule that allows access to the database for the ToroDB Stampede user.

```
  host    torod   torodb      127.0.0.1/32    md5
  host    torod   torodb      ::1/128         md5
```

__Make sure that new rules precede any other rule for same host that apply to all users (eg: 127.0.0.1/32). For more information on `pg_hba.conf` refer to the [Official PostgreSQL documentation](https://www.postgresql.org/docs/current/static/auth-pg-hba-conf.html)__.

## The command wget is not found in macOS

By default macOS hasn't the `wget` tool in the terminal, if you want to use it [Homebrew](http://brew.sh) can be used.

Once installed Homebrew, `wget` can be installed with `brew install wget`.
