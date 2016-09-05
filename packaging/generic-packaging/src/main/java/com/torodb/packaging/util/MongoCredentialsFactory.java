
package com.torodb.packaging.util;

import com.mongodb.MongoCredential;
import com.torodb.core.exceptions.SystemException;
import com.torodb.packaging.config.model.Config;
import com.torodb.packaging.config.model.protocol.mongo.Auth;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.stream.Collectors;

/**
 *
 */
public class MongoCredentialsFactory {

    private MongoCredentialsFactory() {}

    public static MongoCredential getMongoCredential(Config config) {
        MongoCredential mongoCredential = null;

        Auth auth = config.getProtocol().getMongo().getReplication().get(0).getAuth();
        switch (auth.getMode()) {
            case cr:
                mongoCredential
                        = MongoCredential.createMongoCRCredential(auth.getUser(), auth.getSource(), auth.getPassword().toCharArray());
                break;
            case scram_sha1:
                mongoCredential
                        = MongoCredential.createScramSha1Credential(auth.getUser(), auth.getSource(), auth.getPassword().toCharArray());
                break;
            case negotiate:
                mongoCredential
                        = MongoCredential.createCredential(auth.getUser(), auth.getSource(), auth.getPassword().toCharArray());
                break;
            case x509:
                try {
                    String user = auth.getUser();

                    if (user == null) {
                        KeyStore ks = KeyStoreHelper.getKeyStore(
                                config.getProtocol().getMongo().getReplication().get(0).getSsl());
                        X509Certificate certificate = (X509Certificate) ks.getCertificate(
                                ks.aliases().nextElement());
                        user = Arrays.asList(
                                certificate.getSubjectDN().getName().split(",")).stream().map(dn -> dn.trim()).collect(Collectors.joining(","));
                    }

                    mongoCredential = MongoCredential.createMongoX509Credential(user);
                } catch (Exception exception) {
                    throw new SystemException(exception);
                }
                break;
            case disabled:
            default:
                break;
        }
        return mongoCredential;
    }
}
