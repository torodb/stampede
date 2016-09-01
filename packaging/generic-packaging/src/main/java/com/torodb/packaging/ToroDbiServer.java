
package com.torodb.packaging;

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
import java.time.Clock;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadFactory;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.inject.Singleton;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.eightkdata.mongowp.client.wrapper.MongoAuthenticationConfiguration;
import com.eightkdata.mongowp.client.wrapper.MongoAuthenticationMechanism;
import com.eightkdata.mongowp.client.wrapper.MongoClientConfiguration;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.net.HostAndPort;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.ProvisionException;
import com.torodb.backend.guice.BackendModule;
import com.torodb.common.util.ThreadFactoryIdleService;
import com.torodb.concurrent.guice.ConcurrentModule;
import com.torodb.core.BuildProperties;
import com.torodb.core.annotations.ToroDbIdleService;
import com.torodb.core.exceptions.SystemException;
import com.torodb.core.guice.CoreModule;
import com.torodb.d2r.guice.D2RModule;
import com.torodb.metainfo.guice.MetainfModule;
import com.torodb.mongodb.core.MongodServer;
import com.torodb.mongodb.guice.MongoLayerModule;
import com.torodb.mongodb.repl.ReplCoordinator;
import com.torodb.mongodb.repl.guice.MongoDbReplModule;
import com.torodb.mongodb.utils.FilterProvider;
import com.torodb.packaging.config.model.Config;
import com.torodb.packaging.config.model.protocol.mongo.Auth;
import com.torodb.packaging.config.model.protocol.mongo.AuthMode;
import com.torodb.packaging.config.model.protocol.mongo.FilterList;
import com.torodb.packaging.config.model.protocol.mongo.Replication;
import com.torodb.packaging.config.model.protocol.mongo.SSL;
import com.torodb.packaging.config.util.SimpleRegExpDecoder;
import com.torodb.packaging.guice.BackendImplementationModule;
import com.torodb.packaging.guice.ConfigModule;
import com.torodb.packaging.guice.ExecutorServicesModule;
import com.torodb.packaging.guice.PackagingModule;
import com.torodb.torod.TorodServer;
import com.torodb.torod.guice.TorodModule;

/**
 *
 */
@Singleton
public class ToroDbiServer extends ThreadFactoryIdleService {

    private static final Logger LOGGER = LogManager.getLogger(ToroDbServer.class);

    private final BuildProperties buildProperties;
    private final TorodServer torod;
    private final MongodServer mongod;
    private final ReplCoordinator replCoordinator;
    private final ExecutorsService executorsService;

    @Inject
    ToroDbiServer(@ToroDbIdleService ThreadFactory threadFactory, BuildProperties buildProperties,
            TorodServer torod, MongodServer mongod, ReplCoordinator replCoordinator,
            ExecutorsService executorsService) {
        super(threadFactory);
        this.buildProperties = buildProperties;
        this.torod = torod;
        this.mongod = mongod;
        this.replCoordinator = replCoordinator;
        this.executorsService = executorsService;
    }

    public static ToroDbiServer create(Config config, Clock clock) throws ProvisionException {
        Injector injector = createInjector(config, clock);
        return injector.getInstance(ToroDbiServer.class);
    }

    public static Injector createInjector(Config config, Clock clock) {
        List<Replication> replications = config.getProtocol().getMongo().getReplication();
        if (replications == null) {
            throw new IllegalArgumentException("Replication section (protocol.mongo.replication) must be set");
        }
        if (replications.size() != 1) {
            throw new IllegalArgumentException("Exactly one protocol.mongo.replication must be set");
        }
        Replication replication = replications.stream().findAny().get();
        MongoClientConfiguration mongoClientConfiguration = getMongoClientConfiguration(replication);
        FilterProvider filterProvider = getFilterProvider(replication);

        Injector injector = Guice.createInjector(
                new ConfigModule(config),
                new PackagingModule(clock),
                new CoreModule(),
                new BackendImplementationModule(config),
                new BackendModule(),
                new MetainfModule(),
                new D2RModule(),
                new TorodModule(),
                new MongoLayerModule(),
                new MongoDbReplModule(mongoClientConfiguration, filterProvider),
                new ExecutorServicesModule(),
                new ConcurrentModule()
        );
        return injector;
    }

    private static MongoClientConfiguration getMongoClientConfiguration(Replication replication) {
        HostAndPort syncSource = HostAndPort.fromString(replication.getSyncSource())
                .withDefaultPort(27017);
        
        MongoClientConfiguration.Builder mongoClientConfigurationBuilder = 
                new MongoClientConfiguration.Builder(syncSource);
        
        SSL ssl = replication.getSsl();
        mongoClientConfigurationBuilder.setSslEnabled(ssl.getEnabled());
        if (ssl.getEnabled()) {
            try {
                mongoClientConfigurationBuilder.setSslAllowInvalidHostnames(ssl.getAllowInvalidHostnames());
                
                TrustManager[] tms = getTrustManagers(ssl);
                
                KeyManager[] kms = getKeyManagers(ssl);
    
                SSLContext sslContext;
                if (ssl.getFIPSMode()) {
                    sslContext = SSLContext.getInstance("TLS", "SunPKCS11-NSS");
                } else {
                    sslContext = SSLContext.getInstance("TLS");
                }
                sslContext.init(kms, tms, null);
                mongoClientConfigurationBuilder.setSocketFactory(sslContext.getSocketFactory());
            } catch(Exception exception) {
                throw new SystemException(exception);
            }
        }
        
        Auth auth = replication.getAuth();
        if (auth.getMode().isEnabled()) {
            MongoAuthenticationConfiguration mongoAuthenticationConfiguration = getMongoAuthenticationConfiguration(auth, ssl);
            mongoClientConfigurationBuilder.addAuthenticationConfiguration(mongoAuthenticationConfiguration);
        }
        
        return mongoClientConfigurationBuilder.build();
    }

    public static TrustManager[] getTrustManagers(SSL ssl) throws NoSuchAlgorithmException, FileNotFoundException,
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

    public static KeyManager[] getKeyManagers(SSL ssl) throws NoSuchAlgorithmException, FileNotFoundException,
            KeyStoreException, IOException, CertificateException, UnrecoverableKeyException {
        KeyManager[] kms = null;
        if (ssl.getKeyStoreFile() != null) {
            KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
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

    private static KeyStore getKeyStore(SSL ssl) throws FileNotFoundException, KeyStoreException, IOException,
            NoSuchAlgorithmException, CertificateException {
        InputStream is = new FileInputStream(ssl.getKeyStoreFile());
        char[] storePassword = null;

        if (ssl.getKeyStorePassword() != null) {
            storePassword = ssl.getKeyStorePassword().toCharArray();
        }
        
        KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
        ks.load(is, storePassword);
        return ks;
    }

    private final static ImmutableMap<AuthMode, Function<AuthMode, MongoAuthenticationMechanism>> mongoAuthenticationMechanismConverter =
            Maps.immutableEnumMap(ImmutableMap.of(
                    AuthMode.cr, a -> MongoAuthenticationMechanism.cr,
                    AuthMode.scram_sha1, a -> MongoAuthenticationMechanism.scram_sha1,
                    AuthMode.negotiate, a -> MongoAuthenticationMechanism.negotiate,
                    AuthMode.x509, a -> MongoAuthenticationMechanism.x509
                    ));
    
    private static MongoAuthenticationConfiguration getMongoAuthenticationConfiguration(Auth auth, SSL ssl) {
        AuthMode authMode = auth.getMode();
        MongoAuthenticationConfiguration.Builder mongoAuthenticationConfigurationBuilder = 
                new MongoAuthenticationConfiguration.Builder(mongoAuthenticationMechanismConverter.get(authMode).apply(authMode));
        
        mongoAuthenticationConfigurationBuilder.setUser(auth.getUser());
        mongoAuthenticationConfigurationBuilder.setSource(auth.getSource());
        mongoAuthenticationConfigurationBuilder.setPassword(auth.getPassword());
        
        if (authMode == AuthMode.x509 && auth.getUser() == null) {
            try {
                KeyStore ks = getKeyStore(ssl);
                X509Certificate certificate = (X509Certificate) ks.getCertificate(ks.aliases().nextElement());
                mongoAuthenticationConfigurationBuilder.setUser(
                        Arrays.asList(certificate.getSubjectDN().getName().split(",")).stream()
                            .map(dn -> dn.trim()).collect(Collectors.joining(",")));
            } catch(Exception exception) {
                throw new SystemException(exception);
            }
        }
        
        return mongoAuthenticationConfigurationBuilder.build();
    }

    private static FilterProvider getFilterProvider(Replication replication) {
        FilterProvider filterProvider = new FilterProvider(
                convertFilterList(replication.getInclude()), 
                convertFilterList(replication.getExclude()));
        return filterProvider;
    }

    private static ImmutableMap<Pattern, ImmutableList<Pattern>> convertFilterList(
            FilterList filterList) {
        ImmutableMap.Builder<Pattern, ImmutableList<Pattern>> filterBuilder = ImmutableMap.builder();
        
        if (filterList != null) {
            for (Map.Entry<String, List<String>> databaseEntry : filterList.entrySet()) {
                ImmutableList.Builder<Pattern> collectionsBuilder = ImmutableList.builder();
                for (String collection : databaseEntry.getValue()) {
                    collectionsBuilder.add(SimpleRegExpDecoder.decode(collection));
                }
                filterBuilder.put(SimpleRegExpDecoder.decode(databaseEntry.getKey()), collectionsBuilder.build());
            }
        }
        
        return filterBuilder.build();
    }

    @Override
    protected void startUp() throws Exception {
        LOGGER.info("Starting up ToroDB v" +  buildProperties.getFullVersion());

        executorsService.startAsync();
        executorsService.awaitRunning();

        torod.startAsync();
        mongod.startAsync();

        LOGGER.debug("Waiting for Mongod to be running");
        mongod.awaitRunning();

        LOGGER.debug("Waiting for Torod to be running");
        torod.awaitRunning();
        LOGGER.debug("Waiting for Replication to be running");
        replCoordinator.startAsync();
        replCoordinator.awaitRunning();

        LOGGER.debug("ToroDbiServer ready to run");
    }

    @Override
    protected void shutDown() throws Exception {
        LOGGER.info("Shutting down ToroDB");

        replCoordinator.stopAsync();
        replCoordinator.awaitTerminated();

        mongod.stopAsync();
        mongod.awaitTerminated();

        torod.stopAsync();
        torod.awaitTerminated();

        executorsService.stopAsync();
        executorsService.awaitTerminated();

        LOGGER.debug("ToroDBServer shutdown complete");
    }


}
