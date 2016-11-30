<h1>Frequently Asked Questions</h1>

## Which indexes are created?

ToroDB Stampede doesn't support all index types. Some indexes are supported or partialy supported, and other are skipped.
   
  * **Single field indexes**: Are fully supported.
  * **Compound indexes**: Are not supported and are not created.
  * **Multikey indexes**: The only multikey indexes created in ToroDB Stampede are those whose field(s) are in a embedded document. Multikey indexes over scalar values of an array are not created.
  * **Text indexes**: Are not supported and are not created.
  * **2dsphere indexes**: Are not supported and are not created.
  * **2d indexes**: Are not supported and are not created.
  * **Hashed indexes**: Are not supported and are not created.
  
Any created index can be explicitly [excluded in the configuration](installation/configuration.md#exclude-a-mongodb-index)     
    

## The command wget is not found in macOS

By default macOS hasn't the wget tool in the terminal, if you want to use it [Homebrew](http://brew.sh) can be used.

Once installed Homebrew, it can be installed with `brew install wget`.

## No pg_hba.conf entry

Depending on the running Linux distribution and PostgreSQL installation, the error below could appear.

```
FATAL: no pg_hba.conf entry for host "...", user "...", database "...", SSL off
```

This happens because some installations of PostgreSQL are configured with strict security policies. So PostgreSQL reject host connections through TCP. The `pg_hba.conf` file (usually located in the PostgreSQL's data directory or configuration directory) must be edited with a rule that allows access to the database for the ToroDB Stampede user.

```
  host    torod   torodb      127.0.0.1/32    md5
  host    torod   torodb      ::1/128         md5
```

__Make sure that new rules precede any other rule for same host that apply to all users (eg: 127.0.0.1/32). For more informations on `pg_hba.conf` refer to the [Official PostgreSQL documentation](https://www.postgresql.org/docs/current/static/auth-pg-hba-conf.html)__.
