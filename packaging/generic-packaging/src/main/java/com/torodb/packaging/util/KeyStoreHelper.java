
package com.torodb.packaging.util;

import com.torodb.packaging.config.model.protocol.mongo.SSL;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;

/**
 *
 */
public class KeyStoreHelper {

    private KeyStoreHelper() {}

    public static KeyStore getKeyStore(SSL ssl) throws FileNotFoundException, KeyStoreException, IOException,
            NoSuchAlgorithmException, CertificateException {
        try (InputStream is = new FileInputStream(ssl.getKeyStoreFile())) {
            char[] storePassword = null;
    
            if (ssl.getKeyStorePassword() != null) {
                storePassword = ssl.getKeyStorePassword().toCharArray();
            }
    
            KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
            ks.load(is, storePassword);
            return ks;
        }
    }

}
