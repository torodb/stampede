package com.torodb.mongodb.utils.cloner;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.dispatch.ExecutionContexts;
import akka.japi.Pair;
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
import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.Request;
import com.eightkdata.mongowp.server.api.impl.CollectionCommandArgument;
import com.eightkdata.mongowp.server.api.pojos.MongoCursor;
import com.google.common.annotations.Beta;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.net.HostAndPort;
import com.torodb.common.util.RetryHelper.ExceptionHandler;
import com.torodb.common.util.RetryHelper.ThrowExceptionHandler;
import com.torodb.core.concurrent.StreamExecutor;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.retrier.Retrier;
import com.torodb.core.retrier.Retrier.Hint;
import com.torodb.core.retrier.RetrierAbortException;
import com.torodb.core.retrier.RetrierGiveUpException;
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
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.logging.Level;
import java.util.stream.Stream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;

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
public class AkkaDbCloner implements DbCloner {

    private static final Logger LOGGER = LogManager.getLogger(AkkaDbCloner.class);
    /**
     * The executor that will execute each task in which the cloning is divided.
     */
    private final Executor executor;
    private final StreamExecutor streamExecutor;
    /**
     * The number of parallel task that can be used to clone each collection
     */
    private final int maxParallelInsertTasks;
    /**
     * The size of the buffer where documents are stored before being balanced between the
     * insert phases.
     */
    private final int cursorBatchBufferSize;
    /**
     * The number of documents that each transaction will insert.
     */
    private final int insertBufferSize;
    private final CommitHeuristic commitHeuristic;
    private final Clock clock;
    private final Retrier retrier;

    public AkkaDbCloner(Executor executor, int maxParallelInsertTasks, StreamExecutor streamExecutor,
            int cursorBatchBufferSize, int insertBufferSize, CommitHeuristic commitHeuristic, 
            Clock clock, Retrier retrier) {
        this.executor = executor;
        this.streamExecutor = streamExecutor;
        this.maxParallelInsertTasks = maxParallelInsertTasks;
        Preconditions.checkArgument(maxParallelInsertTasks >= 1, "The number of parallel insert "
                + "tasks level must be higher than 0, but " + maxParallelInsertTasks + " was used");
        this.cursorBatchBufferSize = cursorBatchBufferSize;
        Preconditions.checkArgument(cursorBatchBufferSize >= 1, "cursorBatchBufferSize must be "
                + "higher than 0, but " + cursorBatchBufferSize + " was used");
        this.insertBufferSize = insertBufferSize;
        Preconditions.checkArgument(insertBufferSize >= 1, "Insert buffer size must be higher than "
                + "0, but " + insertBufferSize + " was used");
        this.commitHeuristic = commitHeuristic;
        this.clock = clock;
        this.retrier = retrier;
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

        try {
            Stream<Runnable> prepareCollectionRunnables = collsToClone.stream()
                    .map(collEntry -> () -> prepareCollection(localServer, dstDb, collEntry));
            streamExecutor.executeRunnables(prepareCollectionRunnables)
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
                if (cause instanceof RollbackException) {
                    throw new AssertionError("Unexpected rollback exception", cause);
                }
            }
            throw ex;
        }
        
        ActorSystem actorSystem = ActorSystem.create("dbClone", null, null,
                ExecutionContexts.fromExecutor(executor)
        );
        Materializer materializer = ActorMaterializer.create(actorSystem);

        try (MongoConnection remoteConnection = remoteClient.openConnection()) {
            if (opts.isCloneData()) {
                for (Entry entry : collsToClone) {
                    LOGGER.info("Cloning {}.{} into {}.{}", fromDb, entry.getCollectionName(),
                            dstDb, entry.getCollectionName());

                    try {
                        cloneCollection(localServer, remoteConnection, dstDb, opts, materializer, entry);
                    } catch(CompletionException completionException) {
                        if (completionException.getCause() instanceof RollbackException) {
                            throw (RollbackException) completionException.getCause();
                        }
                        
                        throw completionException;
                    }
                }
            }
            if (opts.isCloneIndexes()) {
                for (Entry entry : collsToClone) {
                    cloneIndex(localServer, dstDb, remoteConnection, opts, entry.getCollectionName());
                }
            }
        }
        try {
            Await.result(actorSystem.terminate(), Duration.Inf());
        } catch (Exception ex) {
            throw new CloningException("Error while trying to terminate the ActorSystem", ex);
        }
    }

    private void cloneCollection(MongodServer localServer, MongoConnection remoteConnection,
            String toDb, CloneOptions opts, Materializer materializer, Entry collToClone)
            throws MongoException {

        String collName = collToClone.getCollectionName();

        MongoCursor<BsonDocument> cursor = openCursor(remoteConnection, collName, opts);

        CollectionIterator iterator = new CollectionIterator(cursor, retrier);

        Source<BsonDocument, NotUsed> source = Source.fromIterator(() -> iterator)
                .buffer(cursorBatchBufferSize, OverflowStrategy.backpressure())
                .async();

        Flow<BsonDocument, Pair<Integer, Integer>, NotUsed> inserterFlow;
        if (maxParallelInsertTasks == 1) {
            inserterFlow = createCloneDocsWorker(localServer, toDb, collName);
        }
        else {
            Graph<FlowShape<BsonDocument, Pair<Integer, Integer>>, NotUsed> graph = GraphDSL.create(builder -> {

                UniformFanOutShape<BsonDocument, BsonDocument> balance = builder.add(
                        Balance.create(maxParallelInsertTasks, false)
                );
                UniformFanInShape<Pair<Integer, Integer>, Pair<Integer, Integer>> merge = builder.add(
                        Merge.create(maxParallelInsertTasks, false)
                );

                for (int i = 0; i < maxParallelInsertTasks; i++) {
                    builder.from(balance.out(i))
                            .via(builder.add(
                                    createCloneDocsWorker(localServer, toDb, collName).async())
                            )
                            .toInlet(merge.in(i));
                }
                return FlowShape.of(balance.in(), merge.out());
            });
            inserterFlow = Flow.fromGraph(graph);
        }
        try {
            retrier.retry(() -> {
                try {
                    Pair<Integer, Integer> insertedDocs = source.via(inserterFlow)
                            .toMat(
                                    Sink.fold(new Pair<>(0,0),
                                            (Pair<Integer, Integer> acum, Pair<Integer, Integer> newBatch)
                                                    -> new Pair<>(acum.first() + newBatch.first(), acum.second()
                                                            + newBatch.second())
                                    ),
                                    Keep.right()
                            )
                            .run(materializer)
                            .toCompletableFuture()
                            .join();
                    LOGGER.warn("Requested docs: {}, Inserted: {}", insertedDocs.second(), insertedDocs.first());
                    return Done.getInstance();
                } catch (CompletionException ex) {
                    Throwable cause = ex.getCause();
                    if (cause != null && cause instanceof RollbackException) {
                        throw (RollbackException) cause;
                    }
                    throw ex;
                }
            }, Hint.TIME_SENSIBLE, Hint.INFREQUENT_ROLLBACK);
        } catch (RetrierGiveUpException ex) {
            throw new CloningException(ex);
        }
    }

    private Flow<BsonDocument, Pair<Integer, Integer>, NotUsed> createCloneDocsWorker(MongodServer localServer,
            String toDb, String collection) {
        return Flow.of(BsonDocument.class)
                .grouped(insertBufferSize)
                .map(docs -> {
                    return new Pair<>(
                            insertDocuments(localServer, toDb, collection, docs),
                            docs.size()
                    );
                }
        );
    }

    private int insertDocuments(MongodServer localServer, String toDb, String collection,
            List<BsonDocument> docsToInsert) {

        int maxAttempts = 10;
        int attempts = 1;

        int insertedDocsCounter = 0;

        int docsPerCommit = commitHeuristic.getDocumentsPerCommit();
        LOGGER.debug("Inserting {} documents on commit batches of {}", docsToInsert.size(), docsPerCommit);
        WriteMongodTransaction transaction = createWriteMongodTransaction(localServer);
        try {

            List<BsonDocument> remainingDocs = docsToInsert;

            while (!remainingDocs.isEmpty()) {
                long start = clock.millis();
                int actualBatchSize = Math.min(docsPerCommit, remainingDocs.size());
                List<BsonDocument> currentDocuments = remainingDocs.subList(0, actualBatchSize);
                remainingDocs = remainingDocs.subList(actualBatchSize, remainingDocs.size());

                assert actualBatchSize == currentDocuments.size();
                
                boolean executed = false;
                while (!executed) {
                    try {
                        Status<InsertResult> insertResult = transaction.execute(
                                new Request(toDb, null, true, null),
                                InsertCommand.INSTANCE,
                                new InsertArgument.Builder(collection)
                                .addDocuments(currentDocuments)
                                .setWriteConcern(WriteConcern.fsync())
                                .setOrdered(true)
                                .build()
                        );
                        if (!insertResult.isOK() || insertResult.getResult().getN() != actualBatchSize) {
                            throw new CloningException("Error while inserting a cloned document");
                        }
                        insertedDocsCounter += insertResult.getResult().getN();
                        transaction.commit();

                        long end = clock.millis();

                        commitHeuristic.notifyDocumentInsertionCommit(actualBatchSize, end - start);

                        int newDocsPerCommit = commitHeuristic.getDocumentsPerCommit();
                        if (newDocsPerCommit != docsPerCommit) {
                            LOGGER.debug("Changing commit batch size from {} to {}", docsPerCommit, newDocsPerCommit);
                            docsPerCommit = newDocsPerCommit;
                        }
                        executed = true;
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
            }
        } catch (UserException ex) {
            throw new CloningException("Unexpected error while cloning documents", ex);
        } finally {
            transaction.close();
        }
        return insertedDocsCounter;
    }

    private MongoCursor<BsonDocument> openCursor(MongoConnection remoteConnection, String collection, CloneOptions opts) throws MongoException {
        EnumSet<QueryOption> queryFlags = EnumSet.of(QueryOption.NO_CURSOR_TIMEOUT); //TODO: enable exhaust?
        if (opts.isSlaveOk()) {
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

    private void prepareCollection(MongodServer localServer, String dstDb, Entry colEntry) 
            throws RetrierAbortException {
        try {
            retrier.retry(() -> {
                try (WriteMongodTransaction transaction = createWriteMongodTransaction(localServer)) {
                    dropCollection(transaction, dstDb, colEntry.getCollectionName());
                    createCollection(transaction, dstDb, colEntry.getCollectionName(), colEntry.getCollectionOptions());
                    transaction.commit();
                    return null;
                } catch (UserException ex) {
                    throw new RetrierAbortException("An unexpected user exception was catched", ex);
                }
            });
        } catch (RetrierGiveUpException ex) {
            throw new CloningException(ex);
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

        @Override
        public boolean isClosed() {
            return delegate.isClosed();
        }

    }

    private static class CollectionIterator implements Iterator<BsonDocument> {

        private final MongoCursor<BsonDocument> cursor;
        private final Retrier retrier;
        private static final ExceptionHandler<Boolean, CloningException> HAS_NEXT_HANDLER = (callback, t, i) -> {
            throw new CloningException("Giving up after {} calls to hasNext", t);
        };
        private static final ExceptionHandler<BsonDocument, CloningException> NEXT_HANDLER = (callback, t, i) -> {
            throw new CloningException("Giving up after {} calls to next", t);
        };

        public CollectionIterator(MongoCursor<BsonDocument> cursor, Retrier retrier) {
            this.cursor = cursor;
            this.retrier = retrier;
        }

        @Override
        public boolean hasNext() {
            Callable<Boolean> callable = () -> {
                try {
                    return cursor.hasNext();
                } catch (RuntimeException ex) {
                    throw new RollbackException(ex);
                }
            };
            return retrier.retry(callable, HAS_NEXT_HANDLER, Hint.TIME_SENSIBLE, Hint.INFREQUENT_ROLLBACK);
        }

        @Override
        public BsonDocument next() {
            Callable<BsonDocument> callable = () -> {
                try {
                    return cursor.next();
                } catch (NoSuchElementException ex) {
                    throw ex;
                } catch (RuntimeException ex) {
                    throw new RollbackException(ex);
                }
            };
            return retrier.retry(callable, NEXT_HANDLER, Hint.TIME_SENSIBLE, Hint.INFREQUENT_ROLLBACK);
        }

    }
}
