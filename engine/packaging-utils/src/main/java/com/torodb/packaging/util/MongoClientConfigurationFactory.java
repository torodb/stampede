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

import com.eightkdata.mongowp.client.wrapper.MongoAuthenticationConfiguration;
import com.eightkdata.mongowp.client.wrapper.MongoAuthenticationMechanism;
import com.eightkdata.mongowp.client.wrapper.MongoClientConfiguration;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.net.HostAndPort;
import com.torodb.core.exceptions.SystemException;
import com.torodb.packaging.config.model.protocol.mongo.AbstractReplication;
import com.torodb.packaging.config.model.protocol.mongo.Auth;
import com.torodb.packaging.config.model.protocol.mongo.AuthMode;
import com.torodb.packaging.config.model.protocol.mongo.Ssl;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

public class MongoClientConfigurationFactory {

  @SuppressWarnings("checkstyle:LineLength")
  private static final ImmutableMap<AuthMode, Function<AuthMode, MongoAuthenticationMechanism>> mongoAuthenticationMechanismConverter =
      Maps.immutableEnumMap(ImmutableMap.of(
          AuthMode.cr, a -> MongoAuthenticationMechanism.cr,
          AuthMode.scram_sha1, a -> MongoAuthenticationMechanism.scram_sha1,
          AuthMode.negotiate, a -> MongoAuthenticationMechanism.negotiate,
          AuthMode.x509, a -> MongoAuthenticationMechanism.x509
      ));

  public static MongoClientConfiguration getMongoClientConfiguration(
      AbstractReplication replication) {
    HostAndPort syncSource = HostAndPort.fromString(replication.getSyncSource())
        .withDefaultPort(27017);

    MongoClientConfiguration.Builder mongoClientConfigurationBuilder =
        new MongoClientConfiguration.Builder(syncSource);

    Ssl ssl = replication.getSsl();
    mongoClientConfigurationBuilder.setSslEnabled(ssl.getEnabled());
    if (ssl.getEnabled()) {
      try {
        mongoClientConfigurationBuilder.setSslAllowInvalidHostnames(ssl.getAllowInvalidHostnames());

        TrustManager[] tms = getTrustManagers(ssl);

        KeyManager[] kms = getKeyManagers(ssl);

        SSLContext sslContext;
        if (ssl.getFipsMode()) {
          sslContext = SSLContext.getInstance("TLS", "SunPKCS11-NSS");
        } else {
          sslContext = SSLContext.getInstance("TLS");
        }
        sslContext.init(kms, tms, null);
        mongoClientConfigurationBuilder.setSocketFactory(sslContext.getSocketFactory());
      } catch (CertificateException | KeyManagementException | KeyStoreException
          | UnrecoverableKeyException | NoSuchProviderException | NoSuchAlgorithmException
          | IOException exception) {
        throw new SystemException(exception);
      }
    }

    Auth auth = replication.getAuth();
    if (auth.getMode().isEnabled()) {
      MongoAuthenticationConfiguration mongoAuthenticationConfiguration =
          getMongoAuthenticationConfiguration(auth, ssl);
      mongoClientConfigurationBuilder.addAuthenticationConfiguration(
          mongoAuthenticationConfiguration);
    }

    return mongoClientConfigurationBuilder.build();
  }

  public static TrustManager[] getTrustManagers(Ssl ssl) throws NoSuchAlgorithmException,
      FileNotFoundException,
      CertificateException, KeyStoreException, IOException {
    TrustManager[] tms = null;
    if (ssl.getCaFile() != null) {
      TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory
          .getDefaultAlgorithm());
      try (InputStream is = new FileInputStream(ssl.getCaFile())) {
        CertificateFactory cf = CertificateFactory.getInstance("X.509");
        X509Certificate caCert = (X509Certificate) cf.generateCertificate(is);

        KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
        ks.load(null);
        ks.setCertificateEntry("ca", caCert);

        tmf.init(ks);

        tms = tmf.getTrustManagers();
      }
    } else if (ssl.getTrustStoreFile() != null) {
      TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory
          .getDefaultAlgorithm());
      try (InputStream is = new FileInputStream(ssl.getCaFile())) {
        char[] storePassword = null;

        if (ssl.getTrustStorePassword() != null) {
          storePassword = ssl.getTrustStorePassword().toCharArray();
        }

        KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
        ks.load(is, storePassword);

        tmf.init(ks);

        tms = tmf.getTrustManagers();
      }
    }
    return tms;
  }

  public static KeyManager[] getKeyManagers(Ssl ssl) throws NoSuchAlgorithmException,
      FileNotFoundException,
      KeyStoreException, IOException, CertificateException, UnrecoverableKeyException {
    KeyManager[] kms = null;
    if (ssl.getKeyStoreFile() != null) {
      KeyManagerFactory kmf = KeyManagerFactory.getInstance(
          KeyManagerFactory.getDefaultAlgorithm());
      KeyStore ks = getKeyStore(ssl);

      char[] keyPassword = null;

      if (ssl.getKeyPassword() != null) {
        keyPassword = ssl.getKeyPassword().toCharArray();
      }

      kmf.init(ks, keyPassword);

      kms = kmf.getKeyManagers();
    }
    return kms;
  }

  private static KeyStore getKeyStore(Ssl ssl) throws FileNotFoundException, KeyStoreException,
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

  private static MongoAuthenticationConfiguration getMongoAuthenticationConfiguration(Auth auth,
      Ssl ssl) {
    AuthMode authMode = auth.getMode();
    MongoAuthenticationConfiguration.Builder mongoAuthenticationConfigurationBuilder =
        new MongoAuthenticationConfiguration.Builder(mongoAuthenticationMechanismConverter.get(
            authMode).apply(authMode));

    mongoAuthenticationConfigurationBuilder.setUser(auth.getUser());
    mongoAuthenticationConfigurationBuilder.setSource(auth.getSource());
    mongoAuthenticationConfigurationBuilder.setPassword(auth.getPassword());

    if (authMode == AuthMode.x509 && auth.getUser() == null) {
      try {
        KeyStore ks = getKeyStore(ssl);
        X509Certificate certificate = (X509Certificate) ks
            .getCertificate(ks.aliases().nextElement());
        mongoAuthenticationConfigurationBuilder.setUser(
            Arrays.asList(certificate.getSubjectDN().getName().split(",")).stream()
                .map(dn -> dn.trim()).collect(Collectors.joining(",")));
      } catch (CertificateException | KeyStoreException | NoSuchAlgorithmException
          | IOException exception) {
        throw new SystemException(exception);
      }
    }

    return mongoAuthenticationConfigurationBuilder.build();
  }

}
