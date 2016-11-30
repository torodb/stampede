/*
 * ToroDB
 * Copyright © 2014 8Kdata Technology (www.8kdata.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.torodb.packaging.util;

import com.torodb.packaging.config.model.protocol.mongo.Ssl;

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

  private KeyStoreHelper() {
  }

  public static KeyStore getKeyStore(Ssl ssl) throws FileNotFoundException, KeyStoreException,
      IOException,
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
