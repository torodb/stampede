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

package com.torodb.backend;

import com.torodb.backend.ErrorHandler.Context;
import com.torodb.backend.meta.SchemaUpdater;
import com.torodb.core.annotations.TorodbIdleService;
import com.torodb.core.backend.BackendConnection;
import com.torodb.core.backend.BackendService;
import com.torodb.core.concurrent.ConcurrentToolsFactory;
import com.torodb.core.concurrent.StreamExecutor;
import com.torodb.core.d2r.IdentifierFactory;
import com.torodb.core.d2r.ReservedIdGenerator;
import com.torodb.core.exceptions.SystemException;
import com.torodb.core.exceptions.ToroRuntimeException;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.retrier.Retrier;
import com.torodb.core.retrier.Retrier.Hint;
import com.torodb.core.retrier.RetrierGiveUpException;
import com.torodb.core.services.IdleTorodbService;
import com.torodb.core.transaction.RollbackException;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.core.transaction.metainf.MetaDocPartIndexColumn;
import com.torodb.core.transaction.metainf.MetaIdentifiedDocPartIndex;
import com.torodb.core.transaction.metainf.MetaSnapshot;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.lambda.tuple.Tuple2;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ThreadFactory;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import javax.inject.Inject;

/**
 *
 */
public class BackendServiceImpl extends IdleTorodbService implements BackendService {

  private static final Logger LOGGER = LogManager.getLogger(BackendServiceImpl.class);

  private final DbBackendService dbBackendService;
  private final SqlInterface sqlInterface;
  private final ReservedIdGenerator ridGenerator;
  private final Retrier retrier;
  private final StreamExecutor streamExecutor;
  private final KvMetainfoHandler metainfoHandler;
  private final IdentifierFactory identifierFactory;
  private final SchemaUpdater schemaUpdater;

  /**
   * @param threadFactory          the thread factory that will be used to create the startup and
   *                               shutdown threads
   * @param dbBackendService
   * @param sqlInterface
   * @param schemaUpdater
   * @param metainfoHandler
   * @param identifierFactory
   * @param ridGenerator
   * @param retrier
   * @param concurrentToolsFactory
   */
  @Inject
  public BackendServiceImpl(@TorodbIdleService ThreadFactory threadFactory,
      ReservedIdGenerator ridGenerator, DbBackendService dbBackendService,
      SqlInterface sqlInterface, IdentifierFactory identifierFactory,
      Retrier retrier,
      ConcurrentToolsFactory concurrentToolsFactory,
      KvMetainfoHandler metainfoHandler, SchemaUpdater schemaUpdater) {
    super(threadFactory);

    this.dbBackendService = dbBackendService;
    this.sqlInterface = sqlInterface;
    this.ridGenerator = ridGenerator;
    this.retrier = retrier;
    this.streamExecutor = concurrentToolsFactory.createStreamExecutor("backend-inner-jobs", true);
    this.metainfoHandler = metainfoHandler;
    this.identifierFactory = identifierFactory;
    this.schemaUpdater = schemaUpdater;
  }

  @Override
  public BackendConnection openConnection() {
    return new BackendConnectionImpl(this, sqlInterface, ridGenerator, identifierFactory);
  }

  @Override
  public void enableDataImportMode(MetaSnapshot snapshot) throws RollbackException {
    if (!sqlInterface.getDbBackend().isOnDataInsertMode()) {
      if (snapshot.streamMetaDatabases().findAny().isPresent()) {
        throw new IllegalStateException("Can not disable indexes if any database exists");
      }

      sqlInterface.getDbBackend().enableDataInsertMode();
    }
  }

  @Override
  public void disableDataImportMode(MetaSnapshot snapshot) throws RollbackException {
    if (sqlInterface.getDbBackend().isOnDataInsertMode()) {
      sqlInterface.getDbBackend().disableDataInsertMode();

      //create internal indexes
      Stream<Consumer<DSLContext>> createInternalIndexesJobs = snapshot.streamMetaDatabases()
          .flatMap(
              db -> db.streamMetaCollections().flatMap(
                  col -> col.streamContainedMetaDocParts().flatMap(
                      docPart -> enableInternalIndexJobs(db, col, docPart)
                  )
              )
          );

      //create indexes
      Stream<Consumer<DSLContext>> createIndexesJobs = snapshot.streamMetaDatabases().flatMap(
          db -> db.streamMetaCollections().flatMap(
              col -> enableIndexJobs(db, col)
          )
      );

      //backend specific jobs
      Stream<Consumer<DSLContext>> backendSpecificJobs = sqlInterface.getStructureInterface()
          .streamDataInsertFinishTasks(snapshot).map(job -> {
            return (Consumer<DSLContext>) dsl -> {
              String index = job.apply(dsl);
              LOGGER.info("Task {} completed", index);
            };
          });
      Stream<Consumer<DSLContext>> jobs = Stream
          .concat(createInternalIndexesJobs, createIndexesJobs);
      jobs = Stream.concat(jobs, backendSpecificJobs);
      Stream<Runnable> runnables = jobs.map(this::dslConsumerToRunnable);

      streamExecutor.executeRunnables(runnables)
          .join();
    }
  }

  private Stream<Consumer<DSLContext>> enableInternalIndexJobs(MetaDatabase db, MetaCollection col,
      MetaDocPart docPart) {
    StructureInterface structureInterface = sqlInterface.getStructureInterface();

    Stream<Function<DSLContext, String>> consumerStream;

    if (docPart.getTableRef().isRoot()) {
      consumerStream = structureInterface.streamRootDocPartTableIndexesCreation(
          db.getIdentifier(),
          docPart.getIdentifier(),
          docPart.getTableRef()
      );
    } else {
      MetaDocPart parentDocPart = col.getMetaDocPartByTableRef(
          docPart.getTableRef().getParent().get()
      );
      assert parentDocPart != null;
      consumerStream = structureInterface.streamDocPartTableIndexesCreation(
          db.getIdentifier(),
          docPart.getIdentifier(),
          docPart.getTableRef(),
          parentDocPart.getIdentifier()
      );
    }

    return consumerStream.map(job -> {
      return (Consumer<DSLContext>) dsl -> {
        String index = job.apply(dsl);
        LOGGER.info("Created internal index {} for table {}", index, docPart.getIdentifier());
      };
    });
  }

  private Stream<Consumer<DSLContext>> enableIndexJobs(MetaDatabase db, MetaCollection col) {
    List<Consumer<DSLContext>> consumerList = new ArrayList<>();

    Iterator<? extends MetaDocPart> docPartIterator = col.streamContainedMetaDocParts().iterator();
    while (docPartIterator.hasNext()) {
      MetaDocPart docPart = docPartIterator.next();

      Iterator<? extends MetaIdentifiedDocPartIndex> docPartIndexIterator = docPart.streamIndexes()
          .iterator();
      while (docPartIndexIterator.hasNext()) {
        MetaIdentifiedDocPartIndex docPartIndex = docPartIndexIterator.next();

        consumerList.add(createIndexJob(db, docPart, docPartIndex));
      }
    }

    return consumerList.stream();
  }

  private Consumer<DSLContext> createIndexJob(MetaDatabase db, MetaDocPart docPart,
      MetaIdentifiedDocPartIndex docPartIndex) {
    return dsl -> {
      List<Tuple2<String, Boolean>> columnList = new ArrayList<>(docPartIndex.size());
      for (Iterator<? extends MetaDocPartIndexColumn> indexColumnIterator = docPartIndex
          .iteratorColumns(); indexColumnIterator.hasNext();) {
        MetaDocPartIndexColumn indexColumn = indexColumnIterator.next();
        columnList.add(new Tuple2<>(indexColumn.getIdentifier(), indexColumn.getOrdering()
            .isAscending()));
      }

      try {
        sqlInterface.getStructureInterface().createIndex(
            dsl, docPartIndex.getIdentifier(), db.getIdentifier(), docPart.getIdentifier(),
            columnList,
            docPartIndex.isUnique());
      } catch (UserException userException) {
        throw new SystemException(userException);
      }
      LOGGER.info("Created index {} for table {}", docPartIndex.getIdentifier(), docPart
          .getIdentifier());
    };
  }

  private Runnable dslConsumerToRunnable(Consumer<DSLContext> consumer) {
    return () -> {
      try {
        retrier.retry(() -> {
          try (Connection connection = sqlInterface.getDbBackend().createWriteConnection()) {
            DSLContext dsl = sqlInterface.getDslContextFactory()
                .createDslContext(connection);

            consumer.accept(dsl);
            connection.commit();
            return null;
          } catch (SQLException ex) {
            throw sqlInterface.getErrorHandler().handleException(Context.CREATE_INDEX, ex);
          }
        }, Hint.CRITICAL);
      } catch (RetrierGiveUpException ex) {
        throw new ToroRuntimeException(ex);
      }
    };
  }

  @Override
  protected void startUp() throws Exception {
    LOGGER.debug("Starting backend...");

    streamExecutor.startAsync();
    streamExecutor.awaitRunning();

    LOGGER.trace("Waiting for {} to be running...", dbBackendService);
    dbBackendService.awaitRunning();

    LOGGER.debug("Backend started");
  }

  @Override
  protected void shutDown() throws Exception {
    streamExecutor.stopAsync();
    streamExecutor.awaitTerminated();
  }

  void onConnectionClosed(BackendConnectionImpl connection) {
  }

  KvMetainfoHandler getMetaInfoHandler() {
    return metainfoHandler;
  }

  SchemaUpdater getSchemaUpdater() {
    return schemaUpdater;
  }
}
