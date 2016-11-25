<h1>Appendix</h1>

## CLI options

To execute ToroDb Stampede using the command line, a few options can be given to configure the system.

Usage: `torodb-stampede [options]`

| Option | |
|--------|-|
| --application-name | The application name used by driver to connect. |
| -W, --ask-for-password | Force input of PostgreSQL's database user password. |
| --auth-mode | The authentication mode:<ul><li>disabled: Disable authentication mechanism. No authentication will be done</li><li>negotiate: The client will negotiate best mechanism to authenticate. With server version 3.0 or above, the driver will authenticate using the SCRAM-SHA-1 mechanism. Otherwise, the driver will authenticate using the Challenge Response mechanism</li><li>cr: Challenge Response authentication</li><li>x509: X.509 authentication</li></ul> |
| --auth-source | The source database where the user is present. |
| --auth-user | The user that will be authenticate |
| -b, --backend | Specify the backend to use with default values. |
| --backend-database | The database that will be used. |
| --backend-host | The host or ip that will be used to connect. |
| --backend-port | The port that will be used to connect. |
| --backend-user | The user that will be used to connect. |
| -c, --conf | Configuration file in YAML format. |
| --connection-pool-size | Maximum number of connections to establish to the database. It must be higher or equal than 3. |
| --connection-pool-timeout | The timeout in milliseconds after which retrieve a connection from the pool will fail. |
| --enable-metrics | Enable metrics system. |
| --enable-ssl | Enable SSL/TLS layer. |
| -h, --help | Print help and exit. |
| -hp, --help-param | Print help for all available parameters and exit. | 
| --log-level | Level of log emitted (will overwrite default log4j2 configuration) |
| --log4j2-file | Log4j2 configuration file. |
| --mongopass-file | You can specify a file that use .pgpass syntax: `<host>:<port>:<database>:<user>:<password>` (can have multiple lines) |
| -p, --param | Specify a configuration parameter using <path>=<value> syntax. Use --help-param to see all available parameters. |
| -l, --print-config | Print the configuration in YAML format and exit. |
| -lp, --print-param | Print value for a parameter present at <path> (print an empty string if parameter is not present). Use --help-param to see <path> syntax. |
| -lx, --print-xml-config | Print the configuration in XML format and exit. |
| --repl-set-name | The name of the MongoDB Replica Set where this instance will attach. |
| --ssl-allow-invalid-hostnames | Disable hostname verification. |
| --ssl-ca-file | The path to the Certification Authority in PEM format. |
| --ssl-fips-mode | Enable FIPS 140-2 mode. |
| --ssl-key-password | The password of the private key used to authenticate client. |
| --ssl-key-store-file | The path to the Java Key Store file containing the certificate and private key used to authenticate client. |
| --ssl-key-store-password | The password of the Java Key Store file containing and private key used to authenticate client. |
| --ssl-trust-store-file | The path to the Java Key Store file containing the Certification Authority. If CAFile is specified it will be used instead. |
| --ssl-trust-store-password | The password of the Java Key Store file containing the Certification Authority. |
| --sync-source | The host and port (<host>:<port>) of the node from ToroDB has to replicate. |
| --toropass-file | You can specify a file that use .pgpass syntax: `<host>:<port>:<database>:<user>:<password>` (can have multiple lines) |
| --version | Prints the version. |
| -x, --xml-conf | Configuration file in XML format. |

## Configuration file

Another way to configure the system is through configuration file or setting configuration parameters, using the CLI parameter `-p`. Next sections will provide a list of available parameters with the path that identify it in the configuration (as defined by [JSON pointer specification](https://tools.ietf.org/html/rfc6901)).

### ToroDB logging configuration

| Parameter |  |
|--------|-|
| /logging/log4j2File | Overwrites Log4j2 configuration file with the given one. |
| /logging/level | Overwrites the default level with the given one. |
| /logging/packages/<package-name> | Overwrites the default level for the given package name. | 
| /logging/file | Overwrites the default value for the log output file path. |
| /metricsEnabled | With value `true` enables the metrics system, and `false` disables it. |

### Replication configuration

| Parameter |  |
|--------|-|
| /replication/replSetName | Overwrites the default value of the MongoDB Replica Set used for replication. |
| /replication/syncSource | Overwrites the default connection address for the MongoDB Replica Set used for replication (host:port) |

### Replication SSL configuration

| Parameter |  |
|--------|-|
| /replication/ssl/enabled | If `false` the SSL/TLS layer is disabled if `true` it is enabled. |
| /replication/ssl/allowInvalidHostnames | If `true` hostname verification is disabled, if `false` it is enabled. | 
| /replication/ssl/trustStoreFile | The path to the Java Key Store file containing the Certification Authority. If CAFile is specified it will be used instead. | 
| /replication/ssl/trustStorePassword | The password of the Java Key Store file containing the Certification Authority. |
| /replication/ssl/keyStoreFile | The path to the Java Key Store file containing the certificate and private key used to authenticate client. |
| /replication/ssl/keyStorePassword | The password of the Java Key Store file containing and private key used to authenticate client. |
| /replication/ssl/keyPassword | The password of the private key used to authenticate client. |
| /replication/ssl/fipsMode | If `true` enable FIPS 140-2 mode. |
| /replication/ssl/caFile | The path to the Certification Authority in PEM format. |

### Replication authentication configuration

| Parameter |  |
|--------|-|
| /replication/auth/mode | Specifies the authentication mode, that can take one of the next values.<ul><li>disabled: Disable authentication mechanism.</li><li>negotiate: The client will negotiate best mechanism to authenticate. With server version 3.0 or above, the driver will authenticate using the SCRAM-SHA-1 mechanism. Otherwise, the driver will authenticate using the Challenge Response mechanism.</li><li>cr: Challenge Response authentication</li><li>x509: X.509 authentication</li><li>scram_sha1: SCRAM-SHA-1 SASL authentication</li></ul> |
| /replication/auth/user | User to be authenticated. |
| /replication/auth/source | The source database where the user is present. |
| /replication/include/`<string>` | A map of databases and/or collections and/or indexes to exclusively replicate.<ul><li>Each entry represent a database name under which a list of collection names can be specified.</li><li>Each collection can contain a list of indexes each formed by one or more of those fields:<ul><li>name=<string> the index name</li><li>unqiue=<boolean> true when index is unique, false otherwise</li><li>keys/<string>=<string> the name of the field indexed and the index direction or type</ul><li>Character '\*' can be used to denote "any-character" and character '\' to escape characters.</li></ul> |
| /replication/exclude/`<string>` | A map of databases and/or collections and/or indexes to exclusively replicate.<ul><li>Each entry represent a database name under which a list of collection names can be specified.</li><li>Each collection can contain a list of indexes each formed by one or more of those fields:<ul><li>name=<string> the index name</li><li>unqiue=<boolean> true when index is unique, false otherwise</li><li>keys/<string>=<string> the name of the field indexed and the index direction or type</ul><li>Character '\*' can be used to denote "any-character" and character '\' to escape characters.</li></ul> |
| /replication/mongopassFile | Path to the file with MongoDB access configuration in `.pgpass` syntax. |

### PostgreSQL configuration

| Parameter |  |
|--------|-|
| /backend/postgres/host | The host or ip that will be used to connect. |
| /backend/postgres/port | The port that will be used to connect. |
| /backend/postgres/database | The database that will be used. |
| /backend/postgres/user | The user that will be used to connect. |
| /backend/postgres/toropassFile | Path to the file with PostgreSQL access configuration in  `.pgpass` syntax. |
| /backend/postgres/applicationName | The application name used by driver to connect. |

### ToroDB Stampede pool configuration

| Parameter |  |
|--------|-|
| /backend/pool/connectionPoolTimeout | The timeout in milliseconds after which retrieve a connection from the pool will fail. | 
| /backend/pool/connectionPoolSize |  Maximum number of connections to establish to the database. It must be higher or equal than 3. | 
