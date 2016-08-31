
package com.torodb.packaging.util;

import com.mongodb.MongoClientOptions;
import com.torodb.core.exceptions.SystemException;
import com.torodb.packaging.config.model.Config;
import com.torodb.packaging.config.model.protocol.mongo.SSL;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import javax.net.ssl.*;

/**
 *
 */
public class MongoClientOptionsFactory {

    private MongoClientOptionsFactory() {}

    public static MongoClientOptions getMongoClientOptions(Config config) {
        MongoClientOptions.Builder mongoClientOptionsBuilder = new MongoClientOptions.Builder();
        SSL ssl = config.getProtocol().getMongo().getReplication().get(0).getSsl();
        mongoClientOptionsBuilder.sslEnabled(ssl.getEnabled());
        if (ssl.getEnabled()) {
            try {
                mongoClientOptionsBuilder.sslInvalidHostNameAllowed(ssl.getAllowInvalidHostnames());

                TrustManager[] tms = getTrustManagers(ssl);

                KeyManager[] kms = getKeyManagers(ssl);

                SSLContext sslContext;
                if (ssl.getFIPSMode()) {
                    sslContext = SSLContext.getInstance("TLS", "SunPKCS11-NSS");
                } else {
                    sslContext = SSLContext.getInstance("TLS");
                }
                sslContext.init(kms, tms, null);
                mongoClientOptionsBuilder.socketFactory(sslContext.getSocketFactory());
            } catch(Exception exception) {
                throw new SystemException(exception);
            }
        }
        MongoClientOptions mongoClientOptions = mongoClientOptionsBuilder.build();
        return mongoClientOptions;
    }

    private static TrustManager[] getTrustManagers(SSL ssl) throws NoSuchAlgorithmException, FileNotFoundException,
            CertificateException, KeyStoreException, IOException {
        TrustManager[] tms = null;
        if (ssl.getCAFile() != null) {
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            InputStream is = new FileInputStream(ssl.getCAFile());

            CertificateFactory cf = CertificateFactory.getInstance("X.509");
            X509Certificate caCert = (X509Certificate) cf.generateCertificate(is);

            KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
            ks.load(null);
            ks.setCertificateEntry("ca", caCert);

            tmf.init(ks);

            tms = tmf.getTrustManagers();
        } else if (ssl.getTrustStoreFile() != null) {
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            InputStream is = new FileInputStream(ssl.getCAFile());
            char[] storePassword = null;

            if (ssl.getTrustStorePassword() != null) {
                storePassword = ssl.getTrustStorePassword().toCharArray();
            }

            KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
            ks.load(is, storePassword);

            tmf.init(ks);

            tms = tmf.getTrustManagers();
        }
        return tms;
    }

    private static KeyManager[] getKeyManagers(SSL ssl) throws NoSuchAlgorithmException, FileNotFoundException,
            KeyStoreException, IOException, CertificateException, UnrecoverableKeyException {
        KeyManager[] kms = null;
        if (ssl.getKeyStoreFile() != null) {
            KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            KeyStore ks = KeyStoreHelper.getKeyStore(ssl);

            char[] keyPassword = null;

            if (ssl.getKeyPassword() != null) {
                keyPassword = ssl.getKeyPassword().toCharArray();
            }

            kmf.init(ks, keyPassword);

            kms = kmf.getKeyManagers();
        }
        return kms;
    }
}
