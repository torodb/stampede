
package com.torodb.torod.pipeline.akka;

import akka.stream.Materializer;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import com.google.common.base.Throwables;
import com.torodb.core.d2r.D2RTranslatorFactory;
import com.torodb.core.dsl.backend.BackendConnectionJobFactory;
import com.torodb.core.exceptions.SystemException;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.torod.pipeline.InsertPipeline;
import com.torodb.torod.pipeline.InsertPipelineFactory;
import com.torodb.core.transaction.RollbackException;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MutableMetaCollection;
import com.torodb.torod.pipeline.D2RTranslationBatchFunction;
import com.torodb.torod.pipeline.DefaultToBackendFunction;
import com.torodb.kvdocument.values.KVDocument;
import java.util.concurrent.ExecutionException;
import javax.inject.Inject;
import com.torodb.core.backend.WriteBackendTransaction;

/**
 *
 */
public class AkkaInsertSubscriberFactory implements InsertPipelineFactory {

    private final Materializer materializer;
    private final BackendConnectionJobFactory factory;
    private final MetaDatabase database;
    private final int docBatch = 100;

    @Inject
    public AkkaInsertSubscriberFactory(Materializer materializer,
            BackendConnectionJobFactory factory, MetaDatabase database) {
        this.materializer = materializer;
        this.factory = factory;
        this.database = database;
    }

    @Override
    public InsertPipeline createInsertSubscriber(
            D2RTranslatorFactory translatorFactory,
            MutableMetaCollection mutableMetaCollection,
            WriteBackendTransaction backendConnection) {
        return new AkkaInsertPipeline(translatorFactory, mutableMetaCollection, backendConnection);
    }

    private class AkkaInsertPipeline implements InsertPipeline {
        private final D2RTranslatorFactory translatorFactory;
        private final MutableMetaCollection mutableMetaCollection;
        private final WriteBackendTransaction backendConnection;

        public AkkaInsertPipeline(D2RTranslatorFactory translatorFactory,
                MutableMetaCollection mutableMetaCollection, WriteBackendTransaction backendConnection) {
            this.translatorFactory = translatorFactory;
            this.mutableMetaCollection = mutableMetaCollection;
            this.backendConnection = backendConnection;
        }

        @Override
        public void insert(Iterable<KVDocument> docs) throws UserException {

            D2RTranslationBatchFunction d2rFun
                    = new D2RTranslationBatchFunction(translatorFactory, mutableMetaCollection);
            DefaultToBackendFunction r2BackendFun
                    = new DefaultToBackendFunction(factory, database, mutableMetaCollection);
            try {
                Source.from(docs)
                        .grouped(docBatch)
                        .map((kvList) -> d2rFun.apply(kvList))
                        .mapConcat((collData) -> r2BackendFun.apply(collData))
                        .toMat(Sink.foreach((job) -> job.execute(backendConnection)), Keep.right()).run(materializer)
                        .toCompletableFuture()
                        .get();
            } catch (InterruptedException ex) {
                throw new SystemException("insertion interrupted", ex);
            } catch (ExecutionException ex) {
                Throwable t;
                if (ex.getCause() != null) {
                    t = ex.getCause();
                }
                else {
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
        public void setDocsBatchLength() {
            throw new UnsupportedOperationException("Doc batch lenght is not supported on Akka factory yet.");
        }

    }

}
