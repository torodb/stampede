## CLI options

To execute ToroDb Stampede using the command line, a few options can be given to configure the system.

Usage: `<main class> [options]`

| Option | |
|--------|-|
| --application-name | The application name used by driver to connect. |
| -W, --ask-for-password | Force input of PostgreSQL's database user password. (Default: false) |
| --auth-mode | The authentication mode. |
| --auth-source | The source database where the user is present. |
| --auth-user | The user that will be authenticate |
| -b, --backend | Specify the backend to use with default values. |
| --backend-database | The database that will be used. |
| --backend-host | The host or ip that will be used to connect. |
| --backend-port | The port that will be used to connect. |
| --backend-user | The user that will be used to connect. |
| -c, --conf | Configuration file in YAML format. |
| --connection-pool-size | Maximum number of connections to establish to the database. It must be higher or equal than 3. |
| --connection-pool-timeout | The timeout in milliseconds after which retrieve a connection from the pool will fail.
| --cursor-timeout | The timeout in milliseconds after which an opened cursor will be closed automatically. |
| --enable-metrics | Enable metrics system. |
| --enable-ssl | Enable SSL/TLS layer. |
| --gssapi-host-name | This property is used when the fully qualified domain name (FQDN) of the host is required to properly authenticate. |
| --gssapi-sasl-client-properties |  While rarely needed, this property is used to replace the SasClient properties. |
| --gssapi-service-name |  This property is used when the service's name is different that the default of mongodb. |
| --gssapi-subject | This property is used for overriding the Subject under which GSSAPI authentication executes. |
| -h, --help | Print help and exit. (Default: false) |
| -hp, --help-param | Print help for all available parameters and exit. (Default: false) | 
| --log-level | Level of log emitted (will overwrite default log4j2 configuration) |
| --log4j2-file | Log4j2 configuration file. |
| --mongopass-file | You can specify a file that use .pgpass syntax: `<host>:<port>:<database>:<user>:<password>` (can have multiple lines) |
| -p, --param | Specify a configuration parameter using <path>=<value> syntax. Use --help-param to see all available parameters. |
| -l, --print-config | Print the configuration in YAML format and exit. (Default: false) |
| -lp, --print-param | Print value for a parameter present at <path> (print an empty string if parameter is not present). Use --help-param to see <path> syntax. |
| -lx, --print-xml-config | Print the configuration in XML format and exit. (Default: false) |
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

The other way to configure the system is through configuration file, as the CLI command, it has a few available options.

__TBD__
