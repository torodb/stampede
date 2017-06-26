<h1>Trouble Shooting</h1>

## No pg_hba.conf entry

Depending on the Linux distribution and PostgreSQL installation, the error below can appear.

```
FATAL: no pg_hba.conf entry for host "...", user "...", database "...", SSL off
```

This happens if PostgreSQL is configured with strict security policies and thus rejects connections through TCP. The `pg_hba.conf` file (usually in the PostgreSQL's data directory or configuration directory) must be edited with a rule that allows access to the database for the ToroDB Stampede user.

```
  host    torod   torodb      127.0.0.1/32    md5
  host    torod   torodb      ::1/128         md5
```

__Make sure that new rules precede any other rule for same host that apply to all users (eg: 127.0.0.1/32). For more information on `pg_hba.conf` refer to the [Official PostgreSQL documentation](https://www.postgresql.org/docs/current/static/auth-pg-hba-conf.html)__.

## wget not found on macOS

By default macOS hasn't the `wget` tool in the terminal, if you want to use it [Homebrew](http://brew.sh) can be used.

Once installed Homebrew, `wget` can be installed as follows:

```
brew install wget
```

## Duplication errors in the logs

When Stampede is in recovery mode and, during the data importation, new data are inserted in the source, 
 it's possible that Stampede reapplies the last batch of data. This would lead to error messages similar to:
  
```
  2017-06-21 16:43:03 CEST [21807-1] torodb@torod ERROR:  duplicate key value violates unique constraint "test__id_x_a_idx"
  2017-06-21 16:43:03 CEST [21807-2] torodb@torod DETAIL:  Key (_id_x)=(\x594a858188b38a7816e4cfb9) already exists.
  2017-06-21 16:43:03 CEST [21807-3] torodb@torod CONTEXT:  COPY test, line 1
  2017-06-21 16:43:03 CEST [21807-4] torodb@torod STATEMENT:  COPY "test"."test" ("did","_id_x","x_d","a_s") FROM STDIN

```

  Fortunately, there is nothing to worry about this situation.
  
## Unexpected optime errors

Sometimes, the following error is shown:

```

Unexpected optime for last operation to apply. Expected {t: { "$timestamp": { "t": 1497464377, "i": 12} }, i: 30}, but {t: { "$timestamp": { "t": 1497464377, "i": 6} }, i: 30} found

```

This is due to the way in which the last applied operation time is calculated. 
There is a comparison between a time which has been calculated taking into
account all the operations in the oplog batch, and a time which only took
into account replicated operations (that is, filtering out operations that 
have been excluded by replication filters).

So, when this log appears (DEBUG mode) is because the last operations of an
 oplog batch are operations that are excluded by replication filters.