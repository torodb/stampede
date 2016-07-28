package com.torodb.mongodb.utils;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.dispatch.ExecutionContexts;
import akka.stream.*;
import akka.stream.javadsl.*;
import com.eightkdata.mongowp.Status;
import com.eightkdata.mongowp.WriteConcern;
import com.eightkdata.mongowp.bson.BsonDocument;
import com.eightkdata.mongowp.client.core.MongoClient;
import com.eightkdata.mongowp.client.core.MongoConnection;
import com.eightkdata.mongowp.exceptions.MongoException;
import com.eightkdata.mongowp.exceptions.NotMasterException;
import com.eightkdata.mongowp.messages.request.QueryMessage.QueryOption;
import com.eightkdata.mongowp.messages.request.QueryMessage.QueryOptions;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.admin.CreateCollectionCommand;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.admin.CreateCollectionCommand.CreateCollectionArgument;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.admin.CreateIndexesCommand;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.admin.CreateIndexesCommand.CreateIndexesArgument;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.admin.DropCollectionCommand;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.admin.ListCollectionsCommand.ListCollectionsResult.Entry;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.InsertCommand;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.InsertCommand.InsertArgument;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.InsertCommand.InsertResult;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.pojos.CollectionOptions;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.pojos.CursorResult;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.pojos.IndexOptions;
import com.eightkdata.mongowp.server.api.Request;
import com.eightkdata.mongowp.server.api.impl.CollectionCommandArgument;
import com.eightkdata.mongowp.server.api.pojos.MongoCursor;
import com.google.common.annotations.Beta;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.google.common.net.HostAndPort;
import com.torodb.core.exceptions.ToroRuntimeException;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.transaction.RollbackException;
import com.torodb.mongodb.core.MongodConnection;
import com.torodb.mongodb.core.MongodServer;
import com.torodb.mongodb.core.WriteMongodTransaction;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;

/*
 * TODO: This class must be improved. Cloning is not transactional and it has other problems or
 * things that must be improved
 */
/**
 * This class is used to clone databases using a client, so remote and local databases can
 * be cloned.
 * <p>
 * The process can be executed on a ACID way (using a single transaction) or on a more time efficent
 * way using several threads and connections (and therefore, transactions). The former is specially
 * slow, as usually when a transaction is very long, the efficiency is reduced.
 * <p>
 * This class accepts a transactional policy, a concurrent policy and a commit policy. When the
 * transactional policy allow only one transaction, the concurrent policy allow just one thread and
 * the commit policy only commit once all work is done, then the cloning is done on an ACID
 * transactional way.
 */
@Beta
@Singleton
public class AkkaDbCloner {

    private static final Logger LOGGER = LogManager.getLogger(AkkaDbCloner.class);

    public void cloneDatabase(
            MongodServer mongoServer, int parallelismLevel, int docsPerBatch, boolean commitAfterIndex,
            @Nonnull String dstDb,
            @Nonnull MongoClient remoteClient,
            @Nonnull CloneOptions opts) throws CloningException, NotMasterException, MongoException {
        try (ExecutionContext context = new DefaultExecutionContext(mongoServer, parallelismLevel, docsPerBatch, commitAfterIndex)) {
            cloneDatabase(context, dstDb, remoteClient, opts);
        }
    }

    /**
     *
     * @param executionConfiguration
     * @param dstDb
     * @param remoteClient
     * @param opts
     * @throws CloningException
     * @throws NotMasterException if {@link CloneOptions#getWritePermissionSupplier()
     *                            opts.getWritePermissionSupplier().get()} is evaluated to false
     * @throws MongoException
     */
    public void cloneDatabase(
            @Nonnull ExecutionContext executionConfiguration,
            @Nonnull String dstDb,
            @Nonnull MongoClient remoteClient,
            @Nonnull CloneOptions opts) throws CloningException, NotMasterException, MongoException {
        if (!remoteClient.isRemote() && opts.getDbToClone().equals(dstDb)) {
            LOGGER.warn("Trying to clone a database to itself! Ignoring it");
            return;
        }
        String fromDb = opts.getDbToClone();

        CursorResult<Entry> listCollections;
        try (MongoConnection remoteConnection = remoteClient.openConnection()) {
            listCollections = ListCollectionsRequester.getListCollections(
                    remoteConnection,
                    fromDb,
                    null
            );
        } catch (MongoException ex) {
            throw new CloningException(
                    "It was impossible to get information from the remote server",
                    ex
            );
        }

        if (!opts.getWritePermissionSupplier().get()) {
            throw new NotMasterException("Destiny database cannot be written");
        }

        List<Entry> collsToClone = getCollsToClone(listCollections, fromDb, opts);

        if (!opts.getWritePermissionSupplier().get()) {
            throw new NotMasterException("Destiny database cannot be written "
                    + "after get collections info");
        }

        try {
            List<CompletableFuture<Void>> prepareCollectionFutures = collsToClone.stream()
                    .map(collEntry -> CompletableFuture.runAsync(() ->
                            prepareCollection(executionConfiguration, dstDb, collEntry))
                    )
                    .collect(Collectors.toList());
            CompletableFuture.allOf(prepareCollectionFutures.toArray(new CompletableFuture[prepareCollectionFutures.size()]))
                    .join();
        } catch (CompletionException ex) {
            Throwable cause = ex.getCause();
            if (cause != null) {
                if (cause instanceof CloningException) {
                    throw (CloningException) cause;
                }
                if (cause instanceof MongoException) {
                    throw (MongoException) cause;
                }
            }
            throw ex;
        }
        
        ActorSystem actorSystem = ActorSystem.create("dbClone", null, null,
                ExecutionContexts.fromExecutorService(executionConfiguration.getExecutorService()));
        Materializer materializer = ActorMaterializer.create(actorSystem);

        try (MongoConnection remoteConnection = remoteClient.openConnection()) {
            if (opts.isCloneData()) {
                for (Entry entry : collsToClone) {
                    LOGGER.info("Clonning {}.{} into {}.{}", fromDb, entry.getCollectionName(),
                            dstDb, entry.getCollectionName());

                    cloneCollection(executionConfiguration, remoteConnection, dstDb, opts, materializer, entry);
                }
            }
            if (opts.isCloneIndexes()) {
                for (Entry entry : collsToClone) {
                    cloneIndex(executionConfiguration, dstDb, remoteConnection, opts, entry.getCollectionName());
                }
            }
        }
        try {
            Await.result(actorSystem.terminate(), Duration.Inf());
        } catch (Exception ex) {
            throw new CloningException("Error while trying to terminate the ActorSystem", ex);
        }
    }

    private void cloneCollection(ExecutionContext executionConfiguration, MongoConnection remoteConnection,
            String toDb, CloneOptions opts, Materializer materializer, Entry collToClone)
            throws MongoException {

        int cursorBatchBuffer = 1000;
        String collName = collToClone.getCollectionName();
        int parallelInserts = executionConfiguration.getParallelismLevel();
        assert parallelInserts > 0;

        MongoCursor<BsonDocument> cursor = openCursor(remoteConnection, collName, opts);

        Source<BsonDocument, NotUsed> source = Source.from(cursor)
                .buffer(cursorBatchBuffer, OverflowStrategy.backpressure())
                .async();

        Flow<BsonDocument, NotUsed, NotUsed> inserterFlow;
        if (parallelInserts == 1) {
            inserterFlow = createCloneDocsWorker(executionConfiguration, toDb, collName);
        }
        else {
            inserterFlow = Flow.fromGraph(GraphDSL.create(builder -> {

                UniformFanOutShape<BsonDocument, BsonDocument> balance = builder.add(
                        Balance.create(parallelInserts, false)
                );
                UniformFanInShape<NotUsed, NotUsed> merge = builder.add(
                        Merge.create(parallelInserts, false)
                );

                for (int i = 0; i < parallelInserts; i++) {
                    builder.from(balance.out(i))
                            .via(builder.add(
                                    createCloneDocsWorker(executionConfiguration, toDb, collName).async())
                            )
                            .toInlet(merge.in(i));
                }
                return FlowShape.of(balance.in(), merge.out());
            }));
        }
        source.via(inserterFlow)
                .toMat(Sink.ignore(), Keep.right())
                .run(materializer)
                .toCompletableFuture()
                .join();
    }

    private Flow<BsonDocument, NotUsed, NotUsed> createCloneDocsWorker(
            ExecutionContext executionConfiguration, String toDb, String collection) {
        return Flow.of(BsonDocument.class)
                .grouped(10000)
                .map(docs -> {
                    insertDocuments(executionConfiguration, toDb, collection, docs);
                    return NotUsed.getInstance();
                }
        );
    }

    private void insertDocuments(ExecutionContext executionConfiguration, String toDb,
            String collection, List<BsonDocument> docsToInsert) {

        int maxAttempts = 10;
        int attempts = 1;

        int maxDocBatchSize = executionConfiguration.getCommitPolicy().getDocumentsPerCommit();
        LOGGER.debug("Inserting {} documents on commit batches of {}", docsToInsert.size(), maxDocBatchSize);
        WriteMongodTransaction transaction = executionConfiguration.getTransactionPolicy().consumeTransaction();
        try {

            List<BsonDocument> remainingDocs = docsToInsert;

            while (!remainingDocs.isEmpty()) {
                try {
                    int docsBatchSize = Math.min(maxDocBatchSize, remainingDocs.size());
                    List<BsonDocument> currentDocument = remainingDocs.subList(0, docsBatchSize);
                    remainingDocs = remainingDocs.subList(docsBatchSize, remainingDocs.size());

                    Status<InsertResult> insertResult = transaction.execute(
                            new Request(toDb, null, true, null),
                            InsertCommand.INSTANCE,
                            new InsertArgument.Builder(collection)
                            .addDocuments(currentDocument)
                            .setWriteConcern(WriteConcern.fsync())
                            .setOrdered(true)
                            .build()
                    );
                    if (!insertResult.isOK() || insertResult.getResult().getN() != currentDocument.size()) {
                        throw new CloningException("Error while inserting a cloned document");
                    }
                    transaction.commit();
                } catch (RollbackException ex) {
                    if (attempts < maxAttempts) {
                        LOGGER.debug("Found a rollback exception, trying again for " + attempts + "th time", ex);
                        attempts++;
                        transaction.close();
                        transaction = executionConfiguration.getTransactionPolicy().consumeTransaction();
                    }
                    else {
                        LOGGER.error("Found a rollback exception for {}th time. Aborting", attempts);
                        throw ex;
                    }
                }
            }
        } catch (UserException ex) {
            throw new CloningException("Unexpected error while cloning documents", ex);
        } finally {
            transaction.close();
        }
    }

    private MongoCursor<BsonDocument> openCursor(MongoConnection remoteConnection, String collection, CloneOptions opts) throws MongoException {
        EnumSet<QueryOption> queryFlags = EnumSet.of(QueryOption.NO_CURSOR_TIMEOUT); //TODO: enable exhaust?
        if (opts.slaveOk) {
            queryFlags.add(QueryOption.SLAVE_OK);
        }
        return remoteConnection.query(
                opts.getDbToClone(),
                collection,
                null,
                0,
                0,
                new QueryOptions(queryFlags),
                null,
                null
        );
    }


    private List<Entry> getCollsToClone(CursorResult<Entry> listCollections, String fromDb, CloneOptions opts) {
        List<Entry> collsToClone = new ArrayList<>();
        for (Iterator<Entry> iterator = listCollections.getFirstBatch(); iterator.hasNext();) {
            Entry collEntry = iterator.next();
            String collName = collEntry.getCollectionName();

            if (opts.getCollsToIgnore().contains(collName)) {
                LOGGER.debug("Not cloning {} because is marked as an ignored collection", collName);
                continue;
            }

            if (!NamespaceUtil.isUserWritable(fromDb, collName)) {
                LOGGER.info("Not cloning {} because is a not user writable", collName);
                continue;
            }
            if (NamespaceUtil.isNormal(fromDb, collName)) {
                LOGGER.info("Not cloning {} because it is not normal", collName);
                continue;
            }
            LOGGER.info("Collection {}.{} will be cloned", fromDb, collName);
            collsToClone.add(collEntry);
        }
        return collsToClone;
    }

    private void prepareCollection(ExecutionContext executionConfiguration, String dstDb, Entry colEntry) throws RollbackException {
        try (WriteMongodTransaction transaction = executionConfiguration.getTransactionPolicy().consumeTransaction()) {
            dropCollection(transaction, dstDb, colEntry.getCollectionName());
            createCollection(transaction, dstDb, colEntry.getCollectionName(), colEntry.getCollectionOptions());
            transaction.commit();
        } catch (UserException ex) {
            throw new CloningException("An unexpected user exception was catched", ex);
        }
    }

    private void cloneIndex(
            ExecutionContext executionConfiguration,
            String dstDb,
            MongoConnection remoteConnection,
            CloneOptions opts,
            String fromCol) throws CloningException {
        try (WriteMongodTransaction transaction = executionConfiguration.getTransactionPolicy().consumeTransaction()) {
            String fromDb = opts.getDbToClone();
            HostAndPort remoteAddress = remoteConnection.getClientOwner().getAddress();
            String remoteAddressString = remoteAddress != null ? remoteAddress.toString() : "local";
            LOGGER.info("copying indexes from {}.{} on {} to {}.{} on local server",
                    fromDb,
                    fromCol,
                    remoteAddressString,
                    dstDb,
                    fromCol
            );

            Status<?> status;

            List<IndexOptions> indexes = Lists.newArrayList(
                    ListIndexesRequester.getListCollections(remoteConnection, dstDb, fromCol).getFirstBatch()
            );
            if (indexes.isEmpty()) {
                return;
            }

            status = transaction.execute(
                    new Request(dstDb, null, true, null),
                    CreateIndexesCommand.INSTANCE,
                    new CreateIndexesArgument(
                            fromCol,
                            indexes
                    )
            );
            if (!status.isOK()) {
                throw new CloningException("Error while trying to fetch indexes from remote: "
                        + status);
            }
        } catch (MongoException ex) {
            throw new CloningException("Error while trying to fetch indexes from remote", ex);
        }
    }

    private Status<?> createCollection(
            WriteMongodTransaction transaction,
            String db,
            String collection,
            CollectionOptions options) {
        return transaction.execute(
                new Request(db, null, true, null),
                CreateCollectionCommand.INSTANCE,
                new CreateCollectionArgument(collection, options)
        );
    }

    private Status<?> dropCollection(
            WriteMongodTransaction transaction,
            String db,
            String collection) {
        return transaction.execute(
                new Request(db, null, true, null),
                DropCollectionCommand.INSTANCE,
                new CollectionCommandArgument(collection, DropCollectionCommand.INSTANCE)
        );
    }

    public static class CloneOptions {

        private final boolean cloneData;
        private final boolean cloneIndexes;
        private final boolean slaveOk;
        private final boolean snapshot;
        private final String dbToClone;
        private final Set<String> collsToIgnore;
        private final Supplier<Boolean> writePermissionSupplier;

        public CloneOptions(
                boolean cloneData,
                boolean cloneIndexes,
                boolean slaveOk,
                boolean snapshot,
                String dbToClone,
                Set<String> collsToIgnore,
                Supplier<Boolean> writePermissionSupplier) {
            this.cloneData = cloneData;
            this.cloneIndexes = cloneIndexes;
            this.slaveOk = slaveOk;
            this.snapshot = snapshot;
            this.dbToClone = dbToClone;
            this.collsToIgnore = collsToIgnore;
            this.writePermissionSupplier = writePermissionSupplier;
        }

        /**
         * @return true iff data must be cloned
         */
        public boolean isCloneData() {
            return cloneData;
        }

        /**
         * @return true iff indexes must be cloned
         */
        public boolean isCloneIndexes() {
            return cloneIndexes;
        }

        /**
         * @return true iff is ok to clone from a node that is not master
         */
        public boolean isSlaveOk() {
            return slaveOk;
        }

        /**
         * @return true iff $snapshot must be used
         */
        public boolean isSnapshot() {
            return snapshot;
        }

        /**
         * @return the database that will be cloned
         */
        public String getDbToClone() {
            return dbToClone;
        }

        /**
         * @return a set of collections that will not be cloned
         */
        @Nonnull
        public Set<String> getCollsToIgnore() {
            return collsToIgnore;
        }

        /**
         * @return a supplier that can be used to know if write is allowed on the destiny database
         */
        public Supplier<Boolean> getWritePermissionSupplier() {
            return writePermissionSupplier;
        }

    }

    public static class CloningException extends ToroRuntimeException {

        private static final long serialVersionUID = 1L;

        public CloningException() {
        }

        public CloningException(String message) {
            super(message);
        }

        public CloningException(String message, Throwable cause) {
            super(message, cause);
        }

        public CloningException(Throwable cause) {
            super(cause);
        }

    }

    public static interface ExecutionContext extends AutoCloseable {

        ExecutorService getExecutorService();

        TransactionPolicy getTransactionPolicy();

        CommitPolicy getCommitPolicy();

        /**
         * The number of parallel task that can be used to clone each collection
         *
         * @return
         */
        int getParallelismLevel();

        @Override
        public void close();
    }

    public static interface CommitPolicy {

        int getDocumentsPerCommit();

        boolean shouldCommitAfterIndex();
    }

    public static interface TransactionPolicy {
        WriteMongodTransaction consumeTransaction();
    }

    private static class DefaultExecutionContext implements ExecutionContext {
        private final int parallelismLevel;
        
        private final ExecutorService executorService;
        private final ArrayList<MongodConnection> openConnections = new ArrayList<>();
        private final TransactionPolicy transactionPolicy;
        private final CommitPolicy commitPolicy;

        public DefaultExecutionContext(MongodServer mongoServer, int parallelismLevel, int docsPerBatch, boolean commitAfterIndex) {
            this.parallelismLevel = parallelismLevel;
            
            this.executorService = Executors.newFixedThreadPool(parallelismLevel);
            this.transactionPolicy = () -> {
                MongodConnection connection = mongoServer.openConnection();
                openConnections.add(connection);

                return connection.openWriteTransaction();
            };
            this.commitPolicy = new CommitPolicy() {
                @Override
                public int getDocumentsPerCommit() {
                    return docsPerBatch;
                }

                @Override
                public boolean shouldCommitAfterIndex() {
                    return commitAfterIndex;
                }
            };

        }

        @Override
        public ExecutorService getExecutorService() {
            return executorService;
        }

        @Override
        public TransactionPolicy getTransactionPolicy() {
            return transactionPolicy;
        }

        @Override
        public CommitPolicy getCommitPolicy() {
            return commitPolicy;
        }

        @Override
        public int getParallelismLevel() {
            return parallelismLevel;
        }

        @Override
        public void close() {
            openConnections.forEach(connection -> connection.close());
        }

    }
}
