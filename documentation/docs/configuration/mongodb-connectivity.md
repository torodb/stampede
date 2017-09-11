<h1>Configuring the MongoDB Connection</h1>

Per default, ToroDB Stampede uses no password and no SSL to connect to MongoDB. The `auth` and `SSL` settings can be used to change this behaviour:

The following examples uses cr or scram_sha1 authentication mode and a simple SSL setup:

```json
replication:
  replSetName: rs1
  syncSource: localhost:27017
  auth:
    mode: negotiate
    user: stampede
    source: admin
  ssl:
    enabled: true
    caFile: rootCA.pem
```

## Replicate from a MongoDB Sharded Cluster


In the replication section of the yml config file add a shards item with the list of shards's configurations, one for each shard:

```json
replication:
  replSetName: shard
  shards:
    - replSetName: shard1
      syncSource: localhost:27020
    - replSetName: shard2
      syncSource: localhost:27030
    - replSetName: shard3
      syncSource: localhost:27040
```

If `/replication/shards/<index>/replSetName` is not specified, `/replication/replSetName` will be used. This mechanism of property value merging is valid for properties in the following sections:

* `/replication/shards/<index>/ssl` will default to `/replication/ssl`
* `/replication/shards/<index>/auth` will default to `/replication/auth`

## Connect using Secure Socket Layer

To enable SSL connectivity to MongoDB you have to make sure [MongoDB is correctly configured](https://docs.mongodb.com/manual/tutorial/configure-ssl/). 
If the MongoDB certificate is not issued by a known Certification Authority you have to copy the CA file in a path accessible by ToroDB Stampede. 
For this example we assume the CA file is `rootCA.pem`. For testing purpose you may want to set property `/replication/ssl/allowInvalidHostnames` to `true`
to skip host name validation check for the server certificate.

```json
replication:
  replSetName: rs1
  syncSource: localhost:27017
  ssl:
    enabled: true
    caFile: rootCA.pem
```

To create a self signed Certification Authority private key and certificate and a self signed Server private key and certificate:

```
openssl genrsa -out rootCA.key 2048
openssl req -x509 -new -nodes -key rootCA.key -sha256 -days 1024 -out rootCA.pem  -subj '/C=ES/ST=Spain/L=Madrid/O=8Kdata/CN=8Kdata'
openssl x509 -in rootCA.pem -text -noout
openssl genrsa -out mongodb-server.key
openssl req -new -key mongodb-server.key -out mongodb-server.csr -subj '/C=ES/ST=Spain/L=Madrid/O=8Kdata/CN=localserver/DC=localserver'
openssl x509 -req -in mongodb-server.csr -CA rootCA.pem -CAkey rootCA.key -CAcreateserial -out mongodb-server.crt -days 365 -sha256  -extensions san_env -extfile <(printf "[san_env]\nsubjectAltName=IP:127.0.0.1,DNS:localserver")
openssl x509 -in mongodb-server.crt -text -noout
cat mongodb-server.crt mongodb-server.key > mongodb-server.pem
```

If you prefer you may use a Java Key Store file that contain the same Certification Authority certificate:

```json
replication:
  replSetName: rs1
  syncSource: localhost:27017
  ssl:
    enabled: true
    allowInvalidHostnames: true
    trustStoreFile: rootCA.jks
    trustStorePassword: trustme
```

To import the root certificate into a Java Key Store file:

```
openssl x509 -outform der -in rootCA.pem -out rootCA.der
keytool -import -alias rootCA -keystore rootCA.jks -storepass trustme -trustcacerts -noprompt -file rootCA.der
```

## Authenticate against MongoDB

If [MongoDB is configured to authenticate clients](https://docs.mongodb.com/manual/core/authentication-mechanisms/) you will have to create a user with role `__system` since ToroDB Stampede need to send special read only internal commands
to the MongoDB Replica Set members.

```
db.getSiblingDB("admin").createUser({user: "stampede", pwd: "nosqlonsql", roles: [{role: "__system", db: "admin"}]})
```

You can then configure ToroDB Stampede to authenticate against MongoDB:

```json
replication:
  replSetName: rs1
  syncSource: localhost:27017
  auth:
    mode: negotiate
    user: stampede
    source: admin
```

You will need to add following entry to file `~/.mongopass` (or file specified by `/replication/mongopassFile` property):

```
localhost:27017:admin:stampede:nosqlonsql
```

### X.509 authentication

If [MongoDB is configured to authenticate clients using certificates](https://docs.mongodb.com/manual/core/security-x.509/) you will have to create a client private key and certificate, 
import them in a Java Key Store file and create a user with role `__system` in the `$external` database with name constructed by composing the certificate properties in a particular order (see below).

To create the client private key and certificate.

```
openssl genrsa -out mongodb-client.key
openssl req -new -key mongodb-client.key -out mongodb-client.csr -subj '/C=ES/ST=Spain/L=Madrid/O=8Kdata/CN=localclient/DC=localclient'
openssl x509 -req -in mongodb-client.csr -CA rootCA.pem -CAkey rootCA.key -CAcreateserial -out mongodb-client.crt -days 365 -sha256  -extensions san_env -extfile <(printf "[san_env]\nsubjectAltName=IP:127.0.0.1,DNS:localclient")
openssl x509 -in mongodb-client.crt -text -noout
cat mongodb-client.crt mongodb-client.key > mongodb-client.pem
```

To create the Java Trust Store file `mongodb-client.jks` that contains the client certificate and private key starting from PEM files:

```
openssl pkcs12 -export -in mongodb-client.crt -inkey mongodb-client.key -out mongodb-client.p12 -name localclient -passout file:<(echo nosqlonsql)
keytool -noprompt -importkeystore -deststorepass trustme -destkeypass nosqlonsql -destkeystore mongodb-client.jks -srckeystore mongodb-client.p12 -srcstoretype PKCS12 -srcstorepass nosqlonsql
keytool -list -v -keystore mongodb-client.jks -storepass trustme -keypass nosqlonsql
```

To create the user that is authenticated with X.509 mechanism:

```
db.getSiblingDB("$external").createUser({user: "DC=localclient,CN=localclient,O=8Kdata,L=Madrid,ST=Spain,C=ES", roles: [{role: "__system", db: "admin"}]})
```

You can then configure ToroDB Stampede to authenticate against MongoDB:

```json
replication:
  replSetName: rs1
  syncSource: localhost:27017
  auth:
    mode: x509
  ssl:
    enabled: true
    caFile: rootCA.pem
    keyStoreFile: mongodb-client.jks
    keyStorePassword: trustme
    keyPassword: nosqlonsql
```

* `rootCA.pem` and `rootCA.key` should be the certificate and private key of the same Certification Authority used to generate the server certificate.

In this case you do not have to add any entry to the file `~/.mongopass`.
