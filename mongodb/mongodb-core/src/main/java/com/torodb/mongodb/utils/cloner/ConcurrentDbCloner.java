package com.torodb.mongodb.utils.cloner;

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
import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.Request;
import com.eightkdata.mongowp.server.api.impl.CollectionCommandArgument;
import com.eightkdata.mongowp.server.api.pojos.MongoCursor;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.transaction.RollbackException;
import com.torodb.mongodb.core.MongodConnection;
import com.torodb.mongodb.core.MongodServer;
import com.torodb.mongodb.core.WriteMongodTransaction;
import com.torodb.mongodb.utils.DbCloner;
import com.torodb.mongodb.utils.DbCloner.CloneOptions;
import com.torodb.mongodb.utils.DbCloner.CloningException;
import com.torodb.mongodb.utils.ListCollectionsRequester;
import com.torodb.mongodb.utils.ListIndexesRequester;
import com.torodb.mongodb.utils.NamespaceUtil;
import com.torodb.torod.WriteTorodTransaction;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.*;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
public class ConcurrentDbCloner implements DbCloner {

    private static final Logger LOGGER = LogManager.getLogger(ConcurrentDbCloner.class);
    /**
     * The thread facto that will be used to create the threads that clone the database.
     */
    private final ThreadFactory threadFactory;
    /**
     * The number of parallel task that can be used to clone each collection
     */
    private final int maxParallelInsertTasks;
    /**
     * The number of documents that each transaction will insert.
     */
    private final int insertBufferSize;
    private final CommitHeuristic commitHeuristic;
    private final Clock clock;

    public ConcurrentDbCloner(ThreadFactory threadFactory, int parallelLevel,
            int insertBufferSize, CommitHeuristic commitHeuristic, Clock clock) {
        this.threadFactory = threadFactory;
        this.maxParallelInsertTasks = Math.max(parallelLevel - 1, 1);
        Preconditions.checkArgument(parallelLevel >= 1, "The number of parallel insert "
                + "tasks level must be higher than 0, but " + parallelLevel + " was used");
        this.insertBufferSize = insertBufferSize;
        Preconditions.checkArgument(insertBufferSize >= 1, "Insert buffer size must be higher than "
                + "0, but " + insertBufferSize + " was used");
        this.commitHeuristic = commitHeuristic;
        this.clock = clock;
    }

    @Override
    public void cloneDatabase(String dstDb, MongoClient remoteClient, MongodServer localServer,
            CloneOptions opts) throws CloningException, NotMasterException, MongoException {
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

        prepareCollections(collsToClone, localServer, dstDb);

        if (opts.isCloneData()) {
            cloneCollections(localServer, remoteClient, fromDb, collsToClone, opts);
        }
        if (opts.isCloneIndexes()) {
            for (Entry entry : collsToClone) {
                try (MongoConnection remoteConnection = remoteClient.openConnection()) {
                    cloneIndex(localServer, dstDb, remoteConnection, opts, entry.getCollectionName());
                }
            }
        }
    }

    private void prepareCollections(List<Entry> collsToClone, MongodServer localServer, String dstDb)
            throws MongoException, CloningException {
        int threadNumber = Math.min(collsToClone.size(), 16);
        ExecutorService createCollectionsES = Executors.newFixedThreadPool(
                threadNumber,
                new ThreadFactoryBuilder()
                .setThreadFactory(threadFactory)
                .setNameFormat("cloner-prepare-collection-%d")
                .build()
        );

        try {
            boolean finish = false;
            while (!finish) {
                try {
                    List<CompletableFuture<Void>> prepareCollectionFutures = collsToClone.stream()
                            .map(collEntry -> CompletableFuture.runAsync(()
                                    -> prepareCollection(localServer, dstDb, collEntry),
                                    createCollectionsES
                            ))
                            .collect(Collectors.toList());
                    CompletableFuture.allOf(prepareCollectionFutures.toArray(new CompletableFuture[prepareCollectionFutures.size()]))
                            .join();
                    finish = true;
                } catch (CompletionException ex) {
                    Throwable cause = ex.getCause();
                    if (cause != null) {
                        if (cause instanceof CloningException) {
                            throw (CloningException) cause;
                        }
                        if (cause instanceof MongoException) {
                            throw (MongoException) cause;
                        }
                        if (cause instanceof RollbackException) {
                            continue;
                        }
                    }
                    throw ex;
                }
            }
        } finally {
            createCollectionsES.shutdown();
        }
    }

    private void cloneCollections(MongodServer localServer, MongoClient remoteClient,
            String toDb, List<Entry> collsToClone, CloneOptions opts) throws CompletionException {

        CompletableFuture<List<CompletableFuture<Void>>> futures = new CompletableFuture<>();

        ExecutorService insertExecutor = new ThreadPoolExecutor(
                1,
                maxParallelInsertTasks,
                10,
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(100),
                new ThreadFactoryBuilder()
                .setNameFormat("cloner-insert-%d")
                .build(),
                new ThreadPoolExecutor.CallerRunsPolicy()
        );

        Instant start;
        if (LOGGER.isDebugEnabled()) {
            start = clock.instant();
        } else {
            start = Instant.EPOCH;
        }
        try {
            Thread fetcherThread = threadFactory.newThread(() -> {
                futures.complete(
                        fetchData(remoteClient, collsToClone, insertExecutor, opts, toDb,
                                (collection, docs)
                                -> insertDocuments(localServer, toDb, collection, docs)
                        )
                );
            });
            fetcherThread.setName("cloner-fetch");
            fetcherThread.start();
            fetcherThread.join();

            futures.thenCompose(list ->
                    CompletableFuture.allOf(list.toArray(new CompletableFuture[list.size()])))
                    .join();
            LOGGER.debug(() -> "Data from " + opts.getDbToClone() + " have been cloned on " +
                    Duration.between(start, clock.instant()));
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new CloningException("Interrupted while waiting for a task to finish", ex);
        } finally {
            insertExecutor.shutdown();
        }
    }

    private List<CompletableFuture<Void>> fetchData(MongoClient remoteClient, List<Entry> collsToClone,
            Executor insertExecutor, CloneOptions opts, String toDb, BiConsumer<String, List<BsonDocument>> insertFun) {

        final ArrayList<BsonDocument> docBatch = new ArrayList<>(insertBufferSize);
        final ArrayList<CompletableFuture<Void>> futures = new ArrayList<>(100);

        int docsCount = 0;

        try (MongoConnection remoteConnection = remoteClient.openConnection()) {
            for (Entry entry : collsToClone) {
                assert docBatch.isEmpty();

                LOGGER.info("Clonning {}.{} into {}.{}", opts.getDbToClone(), entry.getCollectionName(),
                        toDb, entry.getCollectionName());
                docBatch.clear();

                String collName = entry.getCollectionName();

                MongoCursor<BsonDocument> cursor = openCursor(remoteConnection, collName, opts);

                Iterator<BsonDocument> cursorIt = cursor.iterator();

                while (cursorIt.hasNext()) {
                    docBatch.add(cursorIt.next());

                    if (docBatch.size() >= insertBufferSize) {
                        docsCount += docBatch.size();
                        futures.add(scheduleInsert(collName, docBatch, insertExecutor, insertFun));
                        docBatch.clear();
                    }
                }
                if (!docBatch.isEmpty()) {
                    docsCount += docBatch.size();
                    futures.add(scheduleInsert(collName, docBatch, insertExecutor, insertFun));
                    docBatch.clear();
                }
                LOGGER.debug("{} documents have been marked to clone from {}.{}", docsCount, opts.getDbToClone(), collName);

                assert docBatch.isEmpty();
            }
        }

        return futures;
    }

    private CompletableFuture<Void> scheduleInsert(String collName,
            List<BsonDocument> docBatch, Executor insertExecutor,
            BiConsumer<String, List<BsonDocument>> insertFun) {
        ArrayList<BsonDocument> subBatch = new ArrayList<>(docBatch);

        return CompletableFuture.runAsync(
                () -> insertFun.accept(collName, subBatch),
                insertExecutor
        );
    }

    private void insertDocuments(MongodServer localServer, String toDb, String collection,
            List<BsonDocument> docsToInsert) {

        int maxAttempts = 10;
        int attempts = 1;

        int docsPerCommit = commitHeuristic.getDocumentsPerCommit();
        LOGGER.debug("Inserting {} documents on commit batches of {}", docsToInsert.size(), docsPerCommit);
        WriteMongodTransaction transaction = createWriteMongodTransaction(localServer);
        try {

            List<BsonDocument> remainingDocs = docsToInsert;

            while (!remainingDocs.isEmpty()) {
                try {
                    long start = clock.millis();
                    int actualBatchSize = Math.min(docsPerCommit, remainingDocs.size());
                    List<BsonDocument> currentDocument = remainingDocs.subList(0, actualBatchSize);
                    remainingDocs = remainingDocs.subList(actualBatchSize, remainingDocs.size());

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

                    long end = clock.millis();

                    commitHeuristic.notifyDocumentInsertionCommit(actualBatchSize, end - start);
                    
                    int newDocsPerCommit = commitHeuristic.getDocumentsPerCommit();
                    if (newDocsPerCommit != docsPerCommit) {
                        LOGGER.debug("Changing commit batch size from {} to {}", docsPerCommit, newDocsPerCommit);
                        docsPerCommit = newDocsPerCommit;
                    }
                } catch (RollbackException ex) {
                    if (attempts < maxAttempts) {
                        LOGGER.debug("Found a rollback exception, trying again for " + attempts + "th time", ex);
                        attempts++;
                        transaction.close();
                        transaction = createWriteMongodTransaction(localServer);
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

    private MongoCursor<BsonDocument> openCursor(MongoConnection remoteConnection, String collection, CloneOptions opts) throws CloningException {
        String db = opts.getDbToClone();
        try {
            EnumSet<QueryOption> queryFlags = EnumSet.of(QueryOption.NO_CURSOR_TIMEOUT); //TODO: enable exhaust?
            if (opts.isSlaveOk()) {
                queryFlags.add(QueryOption.SLAVE_OK);
            }
            return remoteConnection.query(
                    db,
                    collection,
                    null,
                    0,
                    0,
                    new QueryOptions(queryFlags),
                    null,
                    null
            );
        } catch (MongoException ex) {
            throw new CloningException("Error while trying to open a remote cursor on " + db + "."
                    + collection, ex);
        }
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

    private void prepareCollection(MongodServer localServer, String dstDb, Entry colEntry) throws RollbackException {
        try (WriteMongodTransaction transaction = createWriteMongodTransaction(localServer)) {
            dropCollection(transaction, dstDb, colEntry.getCollectionName());
            createCollection(transaction, dstDb, colEntry.getCollectionName(), colEntry.getCollectionOptions());
            transaction.commit();
        } catch (UserException ex) {
            throw new CloningException("An unexpected user exception was catched", ex);
        }
    }

    private void cloneIndex(
            MongodServer localServer,
            String dstDb,
            MongoConnection remoteConnection,
            CloneOptions opts,
            String fromCol) throws CloningException {
        try (WriteMongodTransaction transaction = createWriteMongodTransaction(localServer)) {
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

    private WriteMongodTransaction createWriteMongodTransaction(MongodServer server) {
        MongodConnection connection = server.openConnection();
        WriteMongodTransaction delegateTransaction = connection.openWriteTransaction();
        return new CloseConnectionWriteMongodTransaction(delegateTransaction);
    }

    private static class CloseConnectionWriteMongodTransaction implements WriteMongodTransaction {
        private final WriteMongodTransaction delegate;

        public CloseConnectionWriteMongodTransaction(WriteMongodTransaction delegate) {
            this.delegate = delegate;
        }

        @Override
        public void commit() throws RollbackException, UserException {
            delegate.commit();
        }

        @Override
        public WriteTorodTransaction getTorodTransaction() {
            return delegate.getTorodTransaction();
        }

        @Override
        public MongodConnection getConnection() {
            return delegate.getConnection();
        }

        @Override
        public <Arg, Result> Status<Result> execute(Request req, Command<? super Arg, ? super Result> command, Arg arg)
                throws RollbackException {
            return delegate.execute(req, command, arg);
        }

        @Override
        public Request getCurrentRequest() {
            return delegate.getCurrentRequest();
        }

        @Override
        public void close() {
            delegate.close();
            getConnection().close();
        }

    }

}
