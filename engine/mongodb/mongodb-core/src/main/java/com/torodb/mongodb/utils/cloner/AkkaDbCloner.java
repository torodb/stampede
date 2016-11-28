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

package com.torodb.mongodb.utils.cloner;

import akka.NotUsed;
import akka.japi.Pair;
import akka.japi.tuple.Tuple3;
import akka.stream.ActorMaterializer;
import akka.stream.FlowShape;
import akka.stream.Graph;
import akka.stream.Materializer;
import akka.stream.OverflowStrategy;
import akka.stream.UniformFanInShape;
import akka.stream.UniformFanOutShape;
import akka.stream.javadsl.Balance;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.GraphDSL;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Merge;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import com.eightkdata.mongowp.Status;
import com.eightkdata.mongowp.WriteConcern;
import com.eightkdata.mongowp.bson.BsonDocument;
import com.eightkdata.mongowp.client.core.MongoClient;
import com.eightkdata.mongowp.client.core.MongoConnection;
import com.eightkdata.mongowp.exceptions.MongoException;
import com.eightkdata.mongowp.exceptions.NotMasterException;
import com.eightkdata.mongowp.messages.request.QueryMessage.QueryOption;
import com.eightkdata.mongowp.messages.request.QueryMessage.QueryOptions;
import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.Request;
import com.eightkdata.mongowp.server.api.impl.CollectionCommandArgument;
import com.eightkdata.mongowp.server.api.pojos.MongoCursor;
import com.google.common.annotations.Beta;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.torodb.common.util.RetryHelper.ExceptionHandler;
import com.torodb.concurrent.ActorSystemTorodbService;
import com.torodb.core.concurrent.ConcurrentToolsFactory;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.retrier.Retrier;
import com.torodb.core.retrier.Retrier.Hint;
import com.torodb.core.retrier.RetrierAbortException;
import com.torodb.core.retrier.RetrierGiveUpException;
import com.torodb.core.transaction.RollbackException;
import com.torodb.mongodb.commands.pojos.CollectionOptions;
import com.torodb.mongodb.commands.pojos.CursorResult;
import com.torodb.mongodb.commands.pojos.index.IndexOptions;
import com.torodb.mongodb.commands.signatures.admin.CreateCollectionCommand;
import com.torodb.mongodb.commands.signatures.admin.CreateCollectionCommand.CreateCollectionArgument;
import com.torodb.mongodb.commands.signatures.admin.CreateIndexesCommand;
import com.torodb.mongodb.commands.signatures.admin.CreateIndexesCommand.CreateIndexesArgument;
import com.torodb.mongodb.commands.signatures.admin.CreateIndexesCommand.CreateIndexesResult;
import com.torodb.mongodb.commands.signatures.admin.DropCollectionCommand;
import com.torodb.mongodb.commands.signatures.admin.ListCollectionsCommand.ListCollectionsResult.Entry;
import com.torodb.mongodb.commands.signatures.general.InsertCommand;
import com.torodb.mongodb.commands.signatures.general.InsertCommand.InsertArgument;
import com.torodb.mongodb.commands.signatures.general.InsertCommand.InsertResult;
import com.torodb.mongodb.core.MongodConnection;
import com.torodb.mongodb.core.MongodServer;
import com.torodb.mongodb.core.WriteMongodTransaction;
import com.torodb.mongodb.utils.DbCloner;
import com.torodb.mongodb.utils.ListCollectionsRequester;
import com.torodb.mongodb.utils.ListIndexesRequester;
import com.torodb.mongodb.utils.NamespaceUtil;
import com.torodb.torod.SharedWriteTorodTransaction;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ThreadFactory;

/**
 * This class is used to clone databases using a client, so remote and local databases can be
 * cloned.
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
public class AkkaDbCloner extends ActorSystemTorodbService implements DbCloner {

  private static final Logger LOGGER = LogManager.getLogger(AkkaDbCloner.class);
  /**
   * The number of parallel task that can be used to clone each collection
   */
  private final int maxParallelInsertTasks;
  /**
   * The size of the buffer where documents are stored before being balanced between the insert
   * phases.
   */
  private final int cursorBatchBufferSize;
  private final CommitHeuristic commitHeuristic;
  private final Clock clock;
  private final Retrier retrier;

  public AkkaDbCloner(ThreadFactory threadFactory,
      ConcurrentToolsFactory concurrentToolsFactory,
      int maxParallelInsertTasks, int cursorBatchBufferSize,
      CommitHeuristic commitHeuristic, Clock clock, Retrier retrier) {
    super(threadFactory,
        () -> concurrentToolsFactory.createExecutorService(
            "db-cloner", false),
        "akka-db-cloner"
    );

    this.maxParallelInsertTasks = maxParallelInsertTasks;
    Preconditions.checkArgument(maxParallelInsertTasks >= 1, "The number of parallel insert "
        + "tasks level must be higher than 0, but " + maxParallelInsertTasks + " was used");
    this.cursorBatchBufferSize = cursorBatchBufferSize;
    Preconditions.checkArgument(cursorBatchBufferSize >= 1, "cursorBatchBufferSize must be "
        + "higher than 0, but " + cursorBatchBufferSize + " was used");
    this.commitHeuristic = commitHeuristic;
    this.clock = clock;
    this.retrier = retrier;
  }

  @Override
  protected Logger getLogger() {
    return LOGGER;
  }

  @Override
  public void cloneDatabase(String dstDb, MongoClient remoteClient,
      MongodServer localServer, CloneOptions opts) throws CloningException,
      NotMasterException, MongoException {
    Preconditions.checkState(isRunning(), "This db cloner is not running");

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
      for (Entry entry : collsToClone) {
        prepareCollection(localServer, dstDb, entry);
      }
    } catch (RollbackException ex) {
      throw new AssertionError("Unexpected rollback exception", ex);
    }

    Materializer materializer = ActorMaterializer.create(getActorSystem());

    try (MongoConnection remoteConnection = remoteClient.openConnection()) {
      if (opts.isCloneData()) {
        for (Entry entry : collsToClone) {
          LOGGER.info("Cloning collection data {}.{} into {}.{}",
              fromDb, entry.getCollectionName(), dstDb,
              entry.getCollectionName());

          try {
            cloneCollection(localServer, remoteConnection, dstDb,
                opts, materializer, entry);
          } catch (CompletionException completionException) {
            Throwable cause = completionException.getCause();
            if (cause instanceof RollbackException) {
              throw (RollbackException) cause;
            }

            throw completionException;
          }
        }
      }
      if (opts.isCloneIndexes()) {
        for (Entry entry : collsToClone) {
          LOGGER.info("Cloning collection indexes {}.{} into {}.{}",
              fromDb, entry.getCollectionName(), dstDb,
              entry.getCollectionName());

          try {
            cloneIndex(localServer, dstDb, dstDb, remoteConnection,
                opts, entry.getCollectionName(),
                entry.getCollectionName());
          } catch (CompletionException completionException) {
            Throwable cause = completionException.getCause();
            if (cause instanceof RollbackException) {
              throw (RollbackException) cause;
            }

            throw completionException;
          }
        }
      }
    }
  }

  private void cloneCollection(MongodServer localServer,
      MongoConnection remoteConnection, String toDb, CloneOptions opts,
      Materializer materializer, Entry collToClone) throws MongoException {

    String collName = collToClone.getCollectionName();

    MongoCursor<BsonDocument> cursor = openCursor(remoteConnection, collName, opts);

    CollectionIterator iterator = new CollectionIterator(cursor, retrier);

    Source<BsonDocument, NotUsed> source = Source.fromIterator(() -> iterator)
        .buffer(cursorBatchBufferSize, OverflowStrategy.backpressure())
        .async();

    Flow<BsonDocument, Pair<Integer, Integer>, NotUsed> inserterFlow;
    if (maxParallelInsertTasks == 1) {
      inserterFlow = createCloneDocsWorker(localServer, toDb, collName);
    } else {
      Graph<FlowShape<BsonDocument, Pair<Integer, Integer>>, NotUsed> graph = GraphDSL.create(
          builder -> {
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
      source.via(inserterFlow)
          .fold(new Tuple3<>(0, 0, clock.instant()), (acum, batch) ->
              postInsertFold(toDb, collName, acum, batch))
          .toMat(
              Sink.foreach(tuple -> logCollectionCloning(
                  toDb, collName, tuple.t1(), tuple.t2())),
              Keep.right())
          .run(materializer)
          .toCompletableFuture()
          .join();
    } catch (CompletionException ex) {
      Throwable cause = ex.getCause();
      if (cause != null) {
        throw new CloningException("Error while cloning " + toDb + "." + collName, cause);
      }
      throw ex;
    }
  }

  private Tuple3<Integer, Integer, Instant> postInsertFold(String toDb,
      String toCol, Tuple3<Integer, Integer, Instant> acum,
      Pair<Integer, Integer> newBatch) {
    Instant lastLogInstant = acum.t3();

    long now = clock.millis();
    long millisSinceLastLog = now - lastLogInstant.toEpochMilli();
    if (shouldLogCollectionCloning(millisSinceLastLog)) {
      logCollectionCloning(toDb, toCol, acum.t1(), acum.t2());
      lastLogInstant = Instant.ofEpochMilli(now);
    }
    return new Tuple3<>(
        acum.t1() + newBatch.first(),
        acum.t2() + newBatch.second(),
        lastLogInstant
    );
  }

  private boolean shouldLogCollectionCloning(long millisSinceLog) {
    return millisSinceLog > 10000;
  }

  private void logCollectionCloning(String toDb, String toCol, int insertedDocs,
      int requestedDocs) {
    if (insertedDocs != requestedDocs) {
      throw new AssertionError("Detected aninconsistency between inserted documents ( "
          + insertedDocs + ") andrequested documents to insert (" + requestedDocs + ")");
    }
    LOGGER.info("{} documents have been cloned to {}.{}", insertedDocs, toDb, toCol);
  }

  private Flow<BsonDocument, Pair<Integer, Integer>, NotUsed> createCloneDocsWorker(
      MongodServer localServer, String toDb, String collection) {
    return Flow.of(BsonDocument.class)
        //TODO(gortiz): This is not the best way to use the heuristic,
        //as it only be asked once per collection, but there is no
        //builtin stage that groupes using a dynamic function. This kind
        //of stage is very useful and should be implemented.
        .grouped(commitHeuristic.getDocumentsPerCommit())
        .map(docs -> retrier.retry(
            () -> new Tuple3<>(
                clock.instant(),
                insertDocuments(localServer, toDb, collection, docs),
                docs.size()
            ),
            Hint.FREQUENT_ROLLBACK, Hint.TIME_SENSIBLE
        ))
        .map(tuple -> {
          commitHeuristic.notifyDocumentInsertionCommit(
              tuple.t2(),
              clock.millis() - tuple.t1().toEpochMilli()
          );
          return new Pair<>(tuple.t2(), tuple.t3());
        });
  }

  private int insertDocuments(MongodServer localServer, String toDb, String collection,
      List<BsonDocument> docsToInsert) throws RollbackException {

    try (WriteMongodTransaction transaction = createWriteMongodTransaction(localServer)) {

      Status<InsertResult> insertResult = transaction.execute(
          new Request(toDb, null, true, null),
          InsertCommand.INSTANCE,
          new InsertArgument.Builder(collection)
              .addDocuments(docsToInsert)
              .setWriteConcern(WriteConcern.fsync())
              .setOrdered(true)
              .build()
      );

      if (!insertResult.isOk()) {
        throw new CloningException("Error while inserting a cloned document");
      }
      int insertedDocs = insertResult.getResult().getN();
      if (insertedDocs != docsToInsert.size()) {
        throw new CloningException("Expected to insert "
            + docsToInsert.size() + " but " + insertResult
            + " were inserted");
      }

      transaction.commit();

      return insertedDocs;
    } catch (UserException ex) {
      throw new CloningException("Unexpected error while cloning documents", ex);
    }
  }

  private MongoCursor<BsonDocument> openCursor(MongoConnection remoteConnection, String collection,
      CloneOptions opts) throws MongoException {
    //TODO: enable exhaust?
    EnumSet<QueryOption> queryFlags = EnumSet.of(QueryOption.NO_CURSOR_TIMEOUT);
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

  private List<Entry> getCollsToClone(CursorResult<Entry> listCollections, String fromDb,
      CloneOptions opts) {
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
      if (!opts.getCollectionFilter().test(collName)) {
        LOGGER.info("Not cloning {} because it didn't pass the given filter predicate", collName);
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
          createCollection(transaction, dstDb, colEntry.getCollectionName(), colEntry
              .getCollectionOptions());
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
      String fromDb,
      String dstDb,
      MongoConnection remoteConnection,
      CloneOptions opts,
      String fromCol,
      String toCol) throws CloningException {
    WriteMongodTransaction transaction = createWriteMongodTransaction(localServer);
    try {
      try {
        List<IndexOptions> indexesToClone = getIndexesToClone(Lists.newArrayList(
            ListIndexesRequester.getListCollections(remoteConnection, dstDb, fromCol)
                .getFirstBatch()
        ), dstDb, toCol, fromDb, fromCol, opts);
        if (indexesToClone.isEmpty()) {
          return;
        }

        Status<CreateIndexesResult> status = transaction.execute(
            new Request(dstDb, null, true, null),
            CreateIndexesCommand.INSTANCE,
            new CreateIndexesArgument(
                fromCol,
                indexesToClone
            )
        );
        if (!status.isOk()) {
          throw new CloningException("Error while cloning indexes: " + status.getErrorMsg());
        }
        transaction.commit();
      } catch (UserException | MongoException ex) {
        throw new CloningException("Unexpected error while cloning indexes", ex);
      }
    } finally {
      transaction.close();
    }
  }

  private List<IndexOptions> getIndexesToClone(List<IndexOptions> listindexes, String toDb,
      String toCol, String fromCol, String fromDb, CloneOptions opts) {
    List<IndexOptions> indexesToClone = new ArrayList<>();
    for (Iterator<IndexOptions> iterator = listindexes.iterator(); iterator.hasNext();) {
      IndexOptions indexEntry = iterator.next();
      if (!opts.getIndexFilter().test(toCol, indexEntry.getName(), indexEntry.isUnique(), indexEntry
          .getKeys())) {
        LOGGER.info("Not cloning index {}.{} because it didn't pass the given filter predicate",
            toCol, indexEntry.getName());
        continue;
      }
      LOGGER.info("Index {}.{}.{} will be cloned", fromDb, fromCol, indexEntry.getName());
      indexesToClone.add(indexEntry);
    }
    return indexesToClone;
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
    public SharedWriteTorodTransaction getTorodTransaction() {
      return delegate.getTorodTransaction();
    }

    @Override
    public MongodConnection getConnection() {
      return delegate.getConnection();
    }

    @Override
    public <A, R> Status<R> execute(Request req, Command<? super A, ? super R> command, A arg)
        throws RollbackException {
      return delegate.execute(req, command, arg);
    }

    @Override
    public Request getCurrentRequest() {
      return delegate.getCurrentRequest();
    }

    @Override
    public void rollback() {
      delegate.rollback();
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
    private static final ExceptionHandler<Boolean, CloningException> HAS_NEXT_HANDLER =
        (callback, t, i) -> {
          throw new CloningException("Giving up after {} calls to hasNext", t);
        };
    private static final ExceptionHandler<BsonDocument, CloningException> NEXT_HANDLER = (callback,
        t, i) -> {
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
      return retrier.retry(callable, HAS_NEXT_HANDLER, Hint.TIME_SENSIBLE,
          Hint.INFREQUENT_ROLLBACK);
    }

    @Override
    @SuppressFBWarnings(value = {"IT_NO_SUCH_ELEMENT"},
        justification = "This retrier always throws a CloningException on any exception.")
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
