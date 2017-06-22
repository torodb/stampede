<h1>Configuring the MongoDB Connection</h1>

Per default, ToroDB Stampede uses no password and no SSL to connect to MongoDB. The `auth` and `SSL` settings can be used to change this behaviour:

The following examples uses cr or scram_sha1 authentication mode and a simple SSL setup:

```json
replication:
  replSetName: rs1
  syncSource: localhost:27017
  auth:
    mode: negotiate
    user: mymongouser
    source: mymongosource
  ssl:
    enabled: true
    allowInvalidHostnames: false
    caFile: mycafile.pem
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

If `/replication/shards/<index>/replSetName` is not specified `/replication/replSetName` will be used. This mechanism of property value merging is valid for properties in the following sections:

* `/replication/shards/<index>/ssl` will default to `/replication/ssl`
* `/replication/shards/<index>/auth` will default to `/replication/auth`

## Connect using Secure Socket Layer

To enable SSL connectivity to MongoDB you have to make shure [MongoDB is correctly configured](https://docs.mongodb.com/manual/tutorial/configure-ssl/). 
If the MongoDB certificate is not issued by a known Certification Authority you have to copy the CA file in a path accessible by ToroDB Stamepde. 
For this example we assume this path is `/tmp/mongodb-cert.crt`. For testing purpose you may want to set property `/replication/ssl/allowInvalidHostnames` to `true`
to skip host name validation check for the server certificate.

```json
replication:
  replSetName: rs1
  syncSource: localhost:27017
  ssl:
    enabled: true
    allowInvalidHostnames: true
    caFile: /tmp/mongodb-cert.crt
```

If you prefer you may use a Java Key Store file that contain the same Certification Authority certificate:

```json
replication:
  replSetName: rs1
  syncSource: localhost:27017
  ssl:
    enabled: true
    allowInvalidHostnames: true
    trustStoreFile: /tmp/mongodb-cert.jks
    trustStorePassword: trustme
```

To create a self signed Certification Authority private key and certificate and a self signed Server private key and certificate:

```
openssl genrsa -out /tmp/rootCA.key 2048
openssl req -x509 -new -nodes -key /tmp/rootCA.key -sha256 -days 1024 -out /tmp/rootCA.pem  -subj '/C=ES/ST=Spain/L=Madrid/O=8Kdata/CN=8Kdata'
openssl x509 -in /tmp/rootCA.pem -text -noout
openssl genrsa -out server.key
openssl req -new -key /tmp/mongodb-server.key -out /tmp/mongodb-server.csr -subj '/C=ES/ST=Spain/L=Madrid/O=8Kdata/CN=localserver/DC=localserver'
openssl x509 -req -in /tmp/mongodb-server.csr -CA /tmp/rootCA.pem -CAkey /tmp/rootCA.key -CAcreateserial -out /tmp/mongodb-server.crt -days 365 -sha256  -extensions san_env -extfile <(printf "[san_env]\nsubjectAltName=IP:127.0.0.1,DNS:localserver")
openssl x509 -in /tmp/mongodb-server.crt -text -noout
cat /tmp/mongodb-server.crt /tmp/mongodb-server.key > /tmp/mongodb-server.pem
```

```json
replication:
  replSetName: rs1
  ssl:
    enabled: true
    allowInvalidHostnames: true
    trustStoreFile: /tmp/mongodb-cert.jks
    trustStorePassword: trustme
```

## Authenticate against MongoDB

If [MongoDB is configured to authenticate clients](https://docs.mongodb.com/manual/core/authentication-mechanisms/) you will have to create a user with role `__system` since ToroDB Stempede need to send spcial read only internal commands
to the MongoDB Replica Set members.

```
db.getSiblingDB("admin").createUser({user: "stampede", password: "nosqlonsql", roles: [{role: "__system", db: "admin"}]})
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

You will have to have a file `~/.mongopass` (or file specified by `/replication/mongopassFile` property) containing following line:

```
localhost:27017:admin:stampede:nosqlonsql
```

### X.509 authentication

If [MongoDB is configured to authenticate clients using certificates](https://docs.mongodb.com/manual/core/security-x.509/) you will have to create a client private key and certificate, 
import them in a Java Key Store file and create a user with role `__system` in the `$external` database with name constructed by composing the certificate properties in a particular order (see below).

To create the client private key and certificate.

```
openssl genrsa -out /tmp/mongodb-client.key
openssl req -new -key /tmp/mongodb-client.key -out /tmp/mongodb-client.csr -subj '/C=ES/ST=Spain/L=Madrid/O=8Kdata/CN=localclient/DC=localclient'
openssl x509 -req -in /tmp/mongodb-client.csr -CA /tmp/rootCA.pem -CAkey /tmp/rootCA.key -CAcreateserial -out /tmp/mongodb-client.crt -days 365 -sha256  -extensions san_env -extfile <(printf "[san_env]\nsubjectAltName=IP:127.0.0.1,DNS:localclient")
openssl x509 -in /tmp/mongodb-client.crt -text -noout
cat /tmp/mongodb-client.crt /tmp/mongodb-client.key > /tmp/mongodb-client.pem
```

To create the Java Trust Store file `/tmp/mongodb-client.jks` that contains the client certificate and private key starting from PEM files:

```
openssl pkcs12 -export -in /tmp/mongodb-client.crt -inkey /tmp/mongodb-client.key -out /tmp/mongodb-client.p12 -name localclient -passout file:<(echo nosqlonsql)
keytool -noprompt -importkeystore -deststorepass trustme -destkeypass nosqlonsql -destkeystore /tmp/mongodb-client.jks -srckeystore /tmp/mongodb-client.p12 -srcstoretype PKCS12 -srcstorepass nosqlonsql
keytool -list -v -keystore /tmp/mongodb-client.jks -storepass trustme -keypass nosqlonsql
```

To create the user that authenticate with X.509 mechanism:

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
    caFile: /tmp/mongodb-cert.crt
    keyStoreFile: /tmp/mongodb-client.jks
    keyStorePassword: trustme
    keyPassword: nosqlonsql
```

* `/tmp/rootCA.prm` and `/tmp/rootCA.key` should be the certificate and private key of the same Certification Authority used to generate the server certificate.

In this case you do not have to add any entry to the file `~/.mongopass`.