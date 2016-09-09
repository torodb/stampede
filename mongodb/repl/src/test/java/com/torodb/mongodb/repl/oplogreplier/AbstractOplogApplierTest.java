
package com.torodb.mongodb.repl.oplogreplier;

import com.eightkdata.mongowp.OpTime;
import com.eightkdata.mongowp.annotations.MongoWP;
import com.eightkdata.mongowp.bson.BsonDocument;
import com.eightkdata.mongowp.bson.utils.DefaultBsonValues;
import com.eightkdata.mongowp.server.MongoServerConfig;
import com.eightkdata.mongowp.server.api.oplog.InsertOplogOperation;
import com.eightkdata.mongowp.server.api.oplog.OplogOperation;
import com.eightkdata.mongowp.server.api.oplog.OplogVersion;
import com.eightkdata.mongowp.server.api.oplog.UpdateOplogOperation;
import com.eightkdata.mongowp.server.api.pojos.IteratorMongoCursor;
import com.eightkdata.mongowp.server.api.tools.Empty;
import com.google.common.collect.Lists;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.*;
import com.torodb.backend.derby.guice.DerbyBackendModule;
import com.torodb.backend.driver.derby.DerbyDbBackendConfiguration;
import com.torodb.backend.guice.BackendModule;
import com.torodb.concurrent.DefaultConcurrentToolsFactory;
import com.torodb.concurrent.DefaultConcurrentToolsFactory.BlockerThreadFactoryFunction;
import com.torodb.concurrent.DefaultConcurrentToolsFactory.ForkJoinThreadFactoryFunction;
import com.torodb.concurrent.guice.ConcurrentModule;
import com.torodb.core.BuildProperties;
import com.torodb.core.annotations.ParallelLevel;
import com.torodb.core.annotations.ToroDbIdleService;
import com.torodb.core.annotations.ToroDbRunnableService;
import com.torodb.core.cursors.Cursor;
import com.torodb.core.document.ToroDocument;
import com.torodb.core.guice.CoreModule;
import com.torodb.core.language.AttributeReference;
import com.torodb.core.metrics.MetricsConfig;
import com.torodb.core.metrics.MetricsModule;
import com.torodb.d2r.guice.D2RModule;
import com.torodb.kvdocument.conversion.mongowp.MongoWPConverter;
import com.torodb.kvdocument.values.KVInteger;
import com.torodb.metainfo.guice.MetainfModule;
import com.torodb.mongodb.core.MongodConnection;
import com.torodb.mongodb.core.MongodServer;
import com.torodb.mongodb.core.MongodServerConfig;
import com.torodb.mongodb.core.WriteMongodTransaction;
import com.torodb.mongodb.guice.MongoLayerModule;
import com.torodb.mongodb.repl.OplogManager;
import com.torodb.mongodb.repl.guice.AkkaDbClonerProvider;
import com.torodb.mongodb.repl.guice.DocsPerTransaction;
import com.torodb.mongodb.repl.guice.MongoDbRepl;
import com.torodb.mongodb.repl.guice.MongoDbReplModule.DefaultCommitHeuristic;
import com.torodb.mongodb.repl.oplogreplier.DefaultOplogApplier.BatchLimits;
import com.torodb.mongodb.repl.oplogreplier.OplogApplier.ApplyingJob;
import com.torodb.mongodb.repl.oplogreplier.fetcher.LimitedOplogFetcher;
import com.torodb.mongodb.repl.oplogreplier.fetcher.OplogFetcher;
import com.torodb.mongodb.utils.DbCloner;
import com.torodb.mongodb.utils.cloner.CommitHeuristic;
import com.torodb.torod.ReadOnlyTorodTransaction;
import com.torodb.torod.TorodConnection;
import com.torodb.torod.TorodServer;
import com.torodb.torod.guice.MemoryTorodModule;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinPool.ForkJoinWorkerThreadFactory;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.ThreadFactory;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.eightkdata.mongowp.bson.utils.DefaultBsonValues.newDocument;
import static org.junit.Assert.*;


/**
 *
 */
public abstract class AbstractOplogApplierTest {

    private static final Logger LOGGER = LogManager.getLogger(AbstractOplogApplierTest.class);
    private Injector testInjector;
    private MongodServer mongodServer;
    private OplogApplier oplogApplier;
    private TorodServer torodServer;
    private OplogManager oplogManager;

    public abstract Module getSpecificTestModule();

    @Before
    public void setUp() {
        testInjector = Guice.createInjector(new ReplTestModule(),
                new TestMongoDbReplModule(),
                new CoreModule(),
                new BackendModule(),
                new DerbyBackendModule(),
                new MetainfModule(),
                new D2RModule(),
                new MemoryTorodModule(),
//                new SqlTorodModule(),
                new MongoLayerModule(),
                new MetricsModule(new MetricsConfig() {
                    @Override
                    public Boolean getMetricsEnabled() {
                        return true;
                    }
                }),
                new ConcurrentModule(),
                getSpecificTestModule()
        );
        
        torodServer = testInjector.getInstance(TorodServer.class);
        torodServer.startAsync();

        mongodServer = testInjector.getInstance(MongodServer.class);
        mongodServer.startAsync();
        mongodServer.awaitRunning();

        assert mongodServer.getTorodServer().equals(torodServer);

        torodServer.awaitRunning();

        oplogManager = testInjector.getInstance(OplogManager.class);
        oplogManager.startAsync();
        oplogManager.awaitRunning();

        oplogApplier = testInjector.getInstance(OplogApplier.class);
    }

    @After
    public void tearDown() throws Exception {
        assert oplogApplier != null;
        oplogApplier.close();

        assert oplogManager != null;
        oplogManager.stopAsync();
        oplogManager.awaitTerminated();

        assert mongodServer != null;
        mongodServer.stopAsync();
        mongodServer.awaitTerminated();

        assert torodServer != null;
        torodServer.stopAsync();
        torodServer.awaitTerminated();
    }

    private void executeSimpleTest(OnTransactionConsumer<WriteMongodTransaction> given,
            Stream<OplogOperation> oplog, boolean reapplying,
            OnTransactionConsumer<ReadOnlyTorodTransaction> then) throws Exception {
        executeSimpleTest(given,
                oplog,
                reapplying,
                (ReadOnlyTorodTransaction trans) -> {
                    then.execute(trans);
                    return Empty.getInstance();
        }
        );
    }

    private <R> R executeSimpleTest(OnTransactionConsumer<WriteMongodTransaction> given,
            Stream<OplogOperation> oplog, boolean updatesAsUpsert,
            OnTransactionFunction<ReadOnlyTorodTransaction, R> then) throws Exception {
        try (
                MongodConnection conn = mongodServer.openConnection();
                WriteMongodTransaction trans = conn.openWriteTransaction(true)) {

            given.execute(trans);
            trans.commit();
        }

        OplogFetcher fetcher = createOplogFetcher(oplog);

        ApplyingJob job = oplogApplier.apply(fetcher, new ApplierContext.Builder()
                .setReapplying(true)
                .setUpdatesAsUpserts(updatesAsUpsert)
                .build());
        job.onFinishRaw().join();

        try (TorodConnection conn = torodServer.openConnection();
                ReadOnlyTorodTransaction trans = conn.openReadOnlyTransaction()) {
            return then.execute(trans);
        }
    }

    @Test
    public void emptyTest() throws Exception {
        executeSimpleTest((t) -> {}, Stream.empty(), true, (t) -> {});
    }

    @Test
    public void insertTest() throws Exception{

        String db = "db";
        String col = "col";

        int id = 1;
        KVInteger expectedId = KVInteger.of(1);
        BsonDocument docToInsert = newDocument("_id", DefaultBsonValues.newInt(id));

        OnTransactionConsumer<WriteMongodTransaction> given = (trans) -> {
            trans.getTorodTransaction().insert(db, col, Stream.of(MongoWPConverter.toEagerDocument(docToInsert)));
        };
        Stream<OplogOperation> oplog = Stream.of(
                createInsertOplogOp(db, col, docToInsert)
        );
        OnTransactionFunction<ReadOnlyTorodTransaction, List<ToroDocument>> then = (trans) -> {
            List<ToroDocument> docs = Lists.newArrayList(
                    trans.findByAttRef(db, col, new AttributeReference.Builder().addObjectKey("_id").build(), expectedId)
                    .asDocCursor());
            return docs;
        };
        
        List<ToroDocument> docs = executeSimpleTest(given, oplog, false, then);

        assertEquals("Exactly one document should be stored", 1, docs.size());
    }

    @Test
    public void updateTest_no_upsert() throws Exception {
        String db = "db";
        String col = "col";

        int id1 = 1;
        BsonDocument filter1 = newDocument("_id", DefaultBsonValues.newInt(id1));
        AttributeReference aAttRef = new AttributeReference.Builder().addObjectKey("a").build();

        OnTransactionConsumer<WriteMongodTransaction> given = (trans) -> {
            trans.getTorodTransaction().insert(db, col, Stream.of(MongoWPConverter.toEagerDocument(filter1)));
        };
        Stream<OplogOperation> oplog = Stream.of(
                createUpdateOplogOp(db, col, filter1, false, newDocument("$set", newDocument("a", DefaultBsonValues.newInt(1))))
        );
        OnTransactionConsumer<ReadOnlyTorodTransaction> then = (trans) -> {
            Cursor<ToroDocument> id1Cursor = trans.findByAttRef(db, col, aAttRef, KVInteger.of(id1))
                    .asDocCursor();
            assertTrue("The update with id1 did not modify the state of the database", id1Cursor.hasNext());
            ToroDocument id1Doc = id1Cursor.next();
            assertEquals("The update with id1 lost its _id", KVInteger.of(id1), id1Doc.getRoot().get("_id"));
            assertFalse("There are more documents that fullfil the id1 filter that was expected", id1Cursor.hasNext());

        };
        executeSimpleTest(given, oplog, false, then);
    }

    @Test
    public void updateTest_upsert() throws Exception {
        String db = "db";
        String col = "col";

        int id1 = 1;
        int id2 = 2;
        BsonDocument filter1 = newDocument("_id", DefaultBsonValues.newInt(id1));
        BsonDocument filter2 = newDocument("_id", DefaultBsonValues.newInt(id2));
        AttributeReference aAttRef = new AttributeReference.Builder().addObjectKey("a").build();

        OnTransactionConsumer<WriteMongodTransaction> given = (trans) -> {
            trans.getTorodTransaction().insert(db, col, Stream.of(MongoWPConverter.toEagerDocument(filter1)));
        };
        Stream<OplogOperation> oplog = Stream.of(
                createUpdateOplogOp(db, col, filter1, true, newDocument("$set", newDocument("a", DefaultBsonValues.newInt(1)))),
                createUpdateOplogOp(db, col, filter2, true, newDocument("$set", newDocument("a", DefaultBsonValues.newInt(2))))
        );
        OnTransactionConsumer<ReadOnlyTorodTransaction> then = (trans) -> {
            Cursor<ToroDocument> id1Cursor = trans.findByAttRef(db, col, aAttRef, KVInteger.of(id1))
                    .asDocCursor();
            assertTrue("The update with id1 did not modify the state of the database", id1Cursor.hasNext());
            ToroDocument id1Doc = id1Cursor.next();
            assertEquals("The update with id1 lost its _id", KVInteger.of(id1), id1Doc.getRoot().get("_id"));
            assertFalse("There are more documents that fullfil the id1 filter that was expected", id1Cursor.hasNext());

            Cursor<ToroDocument> id2Cursor = trans.findByAttRef(db, col, aAttRef, KVInteger.of(id2))
                            .asDocCursor();
            assertTrue("The update with id2 did not modify the state of the database", id2Cursor.hasNext());
            ToroDocument id2Doc = id2Cursor.next();
            assertEquals("The update with id2 lost its _id", KVInteger.of(id2), id2Doc.getRoot().get("_id"));
            assertFalse("There are more documents that fullfil the id2 filter that was expected", id2Cursor.hasNext());
        };
        executeSimpleTest(given, oplog, false, then);
    }

    private InsertOplogOperation createInsertOplogOp(String db, String col, BsonDocument doc) {
        return new InsertOplogOperation(
                doc,
                db,
                col,
                new OpTime(Instant.now()),
                0,
                OplogVersion.V1,
                false
        );
    }

    private UpdateOplogOperation createUpdateOplogOp(String db, String col, BsonDocument filter,
            boolean upsert, BsonDocument update) {
        return new UpdateOplogOperation(filter, db, col, new OpTime(Instant.now()), 0,
                OplogVersion.V1, false, update, upsert);
    }

    private OplogFetcher createOplogFetcher(Stream<OplogOperation> opsStream) {
        return new LimitedOplogFetcher(
                new IteratorMongoCursor<>(
                        "local",
                        "oplog.rs",
                        1,
                        HostAndPort.fromParts("localhost", 27017),
                        opsStream.iterator())
        );
    }

    /**
     * Like a {@link Consumer}, but can throw exceptions.
     * @param <T>
     */
    @FunctionalInterface
    public static interface OnTransactionConsumer<T> {
        public void execute(T trans) throws Exception;
    }

    /**
     * Like a {@link Function}, but can throw exceptions.
     * @param <T>
     * @param <R>
     */
    @FunctionalInterface
    public static interface OnTransactionFunction<T, R> {
        public R execute(T trans) throws Exception;
    }

    /**
     * A Guice module that simmulates the modules added on packaging project.
     */
    private static class ReplTestModule extends AbstractModule {

        @Override
        protected void configure() {
            bind(Clock.class)
                    .toInstance(Clock.systemUTC());

            bind(DerbyDbBackendConfiguration.class)
                    .toInstance(new TestDerbyDbBackendConfiguration());
            bind(BuildProperties.class)
                    .toInstance(new TestBuildProperties());

            bind(Integer.class)
                    .annotatedWith(ParallelLevel.class)
                    .toInstance(Runtime.getRuntime().availableProcessors());

            ThreadFactory threadFactory = new ThreadFactoryBuilder()
                    .setNameFormat("torodb-executor-%d")
                    .build();

            bind(ThreadFactory.class)
                    .toInstance(threadFactory);

            bind(ThreadFactory.class)
                    .annotatedWith(ToroDbIdleService.class)
                    .toInstance(threadFactory);

            bind(ThreadFactory.class)
                    .annotatedWith(ToroDbRunnableService.class)
                    .toInstance(threadFactory);

            bind(ThreadFactory.class)
                    .annotatedWith(MongoWP.class)
                    .toInstance(threadFactory);

            bind(ForkJoinWorkerThreadFactory.class)
                    .toInstance(ForkJoinPool.defaultForkJoinWorkerThreadFactory);
            
            bind(MongoServerConfig.class)
                    .toInstance(new MongoServerConfig() {
                        @Override
                        public int getPort() {
                            return 28019;
                        }
                    });
            bind(MongodServerConfig.class)
                    .toInstance(new MongodServerConfig(HostAndPort.fromParts("localhost", 28017)));
        }
    }

    private static class TestDerbyDbBackendConfiguration implements DerbyDbBackendConfiguration {

        @Override
        public boolean inMemory() {
            return true;
        }

        @Override
        public boolean embedded() {
            return true;
        }

        @Override
        public long getCursorTimeout() {
            return 10L * 60 * 1000;
        }

        @Override
        public long getConnectionPoolTimeout() {
            return 10_000;
        }

        @Override
        public int getConnectionPoolSize() {
            return 30;
        }

        @Override
        public int getReservedReadPoolSize() {
            return 10;
        }

        @Override
        public String getUsername() {
            return "torodb";
        }

        @Override
        public String getPassword() {
            return null;
        }

        @Override
        public String getDbHost() {
            return "localhost";
        }

        @Override
        public String getDbName() {
            return "torod";
        }

        @Override
        public int getDbPort() {
            return 1527;
        }

        @Override
        public boolean includeForeignKeys() {
            return false;
        }
    }

    private static class TestMongoDbReplModule extends AbstractModule {

        @Override
        protected void configure() {

            bind(DbCloner.class)
                    .annotatedWith(MongoDbRepl.class)
                    .toProvider(AkkaDbClonerProvider.class);

            bind(OplogApplier.class)
                    .to(DefaultOplogApplier.class)
                    .in(Singleton.class);

            bind(DefaultOplogApplier.BatchLimits.class)
                    .toInstance(new BatchLimits(1000, Duration.ofSeconds(2)));

            bind(CommitHeuristic.class)
                    .to(DefaultCommitHeuristic.class)
                    .in(Singleton.class);

            bind(Integer.class)
                    .annotatedWith(DocsPerTransaction.class)
                    .toInstance(1000);

            bind(ThreadFactory.class)
                        .annotatedWith(MongoDbRepl.class)
                    .toInstance(new ThreadFactoryBuilder()
                            .setNameFormat("repl-unnamed-%d")
                            .build()
                    );
            
            bind(DefaultConcurrentToolsFactory.BlockerThreadFactoryFunction.class)
                    .toInstance(new BlockerThreadFactoryFunction() {
                        @Override
                        public ThreadFactory apply(String prefix) {
                            return new ThreadFactoryBuilder()
                                    .setNameFormat(prefix + " -%d")
                                    .build();
                        }
                    });

            bind(DefaultConcurrentToolsFactory.ForkJoinThreadFactoryFunction.class)
                    .toInstance(new ForkJoinThreadFactoryFunction() {
                        @Override
                        public ForkJoinWorkerThreadFactory apply(String prefix) {
                            return new ForkJoinWorkerThreadFactory() {
                                private volatile int idProvider = 0;

                                @Override
                                public ForkJoinWorkerThread newThread(ForkJoinPool pool) {
                                    ForkJoinWorkerThread newThread
                                            = ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(pool);
                                    int id = idProvider++;
                                    newThread.setName(prefix + '-' + id);
                                    return newThread;
                                }
                            };
                        }
                    });
        }

    }

    private static class TestBuildProperties implements BuildProperties {

        public static final String BUILD_PROPERTIES_FILE = "ToroDB.build.properties";
        public static final Pattern FULL_VERSION_PATTERN
                = Pattern.compile("(\\d+)\\.(\\d+)(?:\\.(\\d+))?(?:-(.+))?");

        private final String fullVersion;
        private final int majorVersion;
        private final int minorVersion;
        private final int subVersion;
        private final String extraVersion;
        private final Instant buildTime;
        private final String gitCommitId;
        private final String gitBranch;
        private final String gitRemoteOriginURL;
        private final String javaVersion;
        private final String javaVendor;
        private final String javaVMSpecificationVersion;
        private final String javaVMVersion;
        private final String osName;
        private final String osArch;
        private final String osVersion;

        public TestBuildProperties() {

            fullVersion = "3.2.0";
            Matcher matcher = FULL_VERSION_PATTERN.matcher(fullVersion);
            if (!matcher.matches()) {
                throw new RuntimeException("Invalid version string '" + fullVersion + "'");
            }
            majorVersion = Integer.parseInt(matcher.group(1));
            minorVersion = Integer.parseInt(matcher.group(2));
            subVersion = matcher.group(3) != null ? Integer.parseInt(matcher.group(3)) : 0;
            extraVersion = matcher.group(4);

            // DateUtils.parseDate may be replaced by SimpleDateFormat if using Java7
            try {
                buildTime = Instant.now();
            } catch (DateTimeParseException e) {
                throw new RuntimeException("buildTimestamp property not in ISO8601 format", e);
            }

            gitCommitId = "aCommitId";
            gitBranch = "aGitBranch";
            gitRemoteOriginURL = "aGitRemoteOriginURL";

            javaVersion = "aJavaVersion";
            javaVendor = "aJavaVendor";
            javaVMSpecificationVersion = "aJavaVMSpecificationVersion";
            javaVMVersion = "aJavaVMVersion";

            osName = "aOsName";
            osArch = "aOsArch";
            osVersion = "aOsVersion";
        }

        @Override
        public String getFullVersion() {
            return fullVersion;
        }

        @Override
        public int getMajorVersion() {
            return majorVersion;
        }

        @Override
        public int getMinorVersion() {
            return minorVersion;
        }

        @Override
        public int getSubVersion() {
            return subVersion;
        }

        @Override
        public String getExtraVersion() {
            return extraVersion;
        }

        @Override
        public Instant getBuildTime() {
            return buildTime;
        }

        @Override
        public String getGitCommitId() {
            return gitCommitId;
        }

        @Override
        public String getGitBranch() {
            return gitBranch;
        }

        public String getGitRemoteOriginURL() {
            return gitRemoteOriginURL;
        }

        @Override
        public String getJavaVersion() {
            return javaVersion;
        }

        @Override
        public String getJavaVendor() {
            return javaVendor;
        }

        @Override
        public String getJavaVMSpecificationVersion() {
            return javaVMSpecificationVersion;
        }

        @Override
        public String getJavaVMVersion() {
            return javaVMVersion;
        }

        @Override
        public String getOsName() {
            return osName;
        }

        @Override
        public String getOsArch() {
            return osArch;
        }

        @Override
        public String getOsVersion() {
            return osVersion;
        }
    }
}
