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

package com.torodb.torod.pipeline.impl;

import akka.actor.ActorSystem;
import akka.dispatch.ExecutionContexts;
import akka.stream.ActorMaterializer;
import akka.stream.ActorMaterializerSettings;
import akka.stream.Materializer;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import com.google.common.base.Throwables;
import com.torodb.core.backend.WriteBackendTransaction;
import com.torodb.core.concurrent.ConcurrentToolsFactory;
import com.torodb.core.d2r.D2RTranslatorFactory;
import com.torodb.core.dsl.backend.BackendTransactionJobFactory;
import com.torodb.core.exceptions.SystemException;
import com.torodb.core.exceptions.SystemInterruptedException;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.services.IdleTorodbService;
import com.torodb.core.transaction.RollbackException;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MutableMetaCollection;
import com.torodb.kvdocument.values.KvDocument;
import com.torodb.torod.pipeline.D2RTranslationBatchFunction;
import com.torodb.torod.pipeline.DefaultToBackendFunction;
import com.torodb.torod.pipeline.InsertPipeline;
import com.torodb.torod.pipeline.InsertPipelineFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.stream.Stream;

import javax.inject.Inject;

/**
 *
 */
public class AkkaInsertPipelineFactory extends IdleTorodbService
    implements InsertPipelineFactory {

  private static final Logger LOGGER =
      LogManager.getLogger(AkkaInsertPipelineFactory.class);
  private final ConcurrentToolsFactory concurrentToolsFactory;
  private ActorSystem actorSystem;
  private Materializer materializer;
  private final BackendTransactionJobFactory factory;
  private final int docBatch;
  private ExecutorService executorService;

  @Inject
  public AkkaInsertPipelineFactory(ThreadFactory threadFactory,
      ConcurrentToolsFactory concurrentToolsFactory,
      BackendTransactionJobFactory factory, int docBatch) {
    super(threadFactory);
    this.concurrentToolsFactory = concurrentToolsFactory;
    this.factory = factory;
    this.docBatch = docBatch;
  }

  @Override
  protected void startUp() throws Exception {
    executorService = concurrentToolsFactory.createExecutorService(
        "insert-pipeline",
        true
    );
    actorSystem = ActorSystem.create("insert-pipeline", null, null,
        ExecutionContexts.fromExecutor(executorService)
    );
    this.materializer = ActorMaterializer.create(
        ActorMaterializerSettings.create(actorSystem),
        actorSystem,
        "insert"
    );
  }

  @Override
  protected void shutDown() throws Exception {
    if (actorSystem != null) {
      try {
        Await.result(actorSystem.terminate(), Duration.Inf());
      } catch (Exception ex) {
        throw new RuntimeException("It was impossible to shutdown the "
            + "insert-pipeline actor system", ex);
      }
    }
    if (executorService != null) {
      executorService.shutdown();
    }
    LOGGER.debug("Insert pipeline actor system terminated");
  }

  @Override
  public InsertPipeline createInsertPipeline(
      D2RTranslatorFactory translatorFactory,
      MetaDatabase metaDb,
      MutableMetaCollection mutableMetaCollection,
      WriteBackendTransaction backendConnection,
      boolean concurrent) {
    if (!concurrent) {
      LOGGER.debug("Akka insert pipeline has been used when concurrent "
          + "hint is marked as false. It will be ignored");
    }
    return new AkkaInsertPipeline(translatorFactory, metaDb, mutableMetaCollection,
        backendConnection);
  }

  private class AkkaInsertPipeline implements InsertPipeline {

    private final D2RTranslatorFactory translatorFactory;
    private final MetaDatabase metaDb;
    private final MutableMetaCollection mutableMetaCollection;
    private final WriteBackendTransaction backendConnection;

    public AkkaInsertPipeline(D2RTranslatorFactory translatorFactory, MetaDatabase metaDb,
        MutableMetaCollection mutableMetaCollection, WriteBackendTransaction backendConnection) {
      this.translatorFactory = translatorFactory;
      this.metaDb = metaDb;
      this.mutableMetaCollection = mutableMetaCollection;
      this.backendConnection = backendConnection;
    }

    @Override
    public void insert(Stream<KvDocument> docs) throws UserException {

      D2RTranslationBatchFunction d2rFun =
          new D2RTranslationBatchFunction(translatorFactory, metaDb, mutableMetaCollection);
      DefaultToBackendFunction r2BackendFun =
          new DefaultToBackendFunction(factory, metaDb, mutableMetaCollection);
      try {
        Source.fromIterator(() -> docs.iterator())
            .grouped(docBatch)
            .map(d2rFun::apply)
            .mapConcat(r2BackendFun::apply)
            .async()
            .toMat(
                Sink.foreach((job) -> job.execute(backendConnection)),
                Keep.right())
            .run(materializer)
            .toCompletableFuture()
            .get();
      } catch (InterruptedException ex) {
        throw new SystemInterruptedException("insertion interrupted", ex);
      } catch (ExecutionException ex) {
        Throwable t;
        if (ex.getCause() != null) {
          t = ex.getCause();
        } else {
          t = ex;
        }
        Throwables.propagateIfPossible(t, UserException.class, RollbackException.class);
        throw new SystemException("Execution exception while trying to insert", t);
      }
    }

    @Override
    public int getDocsBatchLength() {
      return docBatch;
    }

    @Override
    public void setDocsBatchLength(int newBatchLength) {
      throw new UnsupportedOperationException(
          "Doc batch lenght is not supported on Akka factory yet.");
    }

  }

}
