
package com.torodb.torod.pipeline.akka;

import akka.stream.Materializer;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import com.google.common.base.Throwables;
import com.torodb.core.backend.WriteBackendTransaction;
import com.torodb.core.d2r.D2RTranslatorFactory;
import com.torodb.core.dsl.backend.BackendTransactionJobFactory;
import com.torodb.core.exceptions.SystemInterruptedException;
import com.torodb.core.exceptions.SystemException;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.transaction.RollbackException;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MutableMetaCollection;
import com.torodb.kvdocument.values.KVDocument;
import com.torodb.torod.guice.TorodLayer;
import com.torodb.torod.pipeline.D2RTranslationBatchFunction;
import com.torodb.torod.pipeline.DefaultToBackendFunction;
import com.torodb.torod.pipeline.InsertPipeline;
import com.torodb.torod.pipeline.InsertPipelineFactory;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;
import javax.inject.Inject;

/**
 *
 */
public class AkkaInsertSubscriberFactory implements InsertPipelineFactory {

    private final Materializer materializer;
    private final BackendTransactionJobFactory factory;
    private final int docBatch;

    @Inject
    public AkkaInsertSubscriberFactory(@TorodLayer Materializer materializer,
            BackendTransactionJobFactory factory, int docBatch) {
        this.materializer = materializer;
        this.factory = factory;
        this.docBatch = docBatch;
    }

    @Override
    public InsertPipeline createInsertPipeline(
            D2RTranslatorFactory translatorFactory,
            MetaDatabase metaDb,
            MutableMetaCollection mutableMetaCollection,
            WriteBackendTransaction backendConnection) {
        return new AkkaInsertPipeline(translatorFactory, metaDb, mutableMetaCollection, backendConnection);
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
        public void insert(Stream<KVDocument> docs) throws UserException {

            D2RTranslationBatchFunction d2rFun
                    = new D2RTranslationBatchFunction(translatorFactory, metaDb, mutableMetaCollection);
            DefaultToBackendFunction r2BackendFun
                    = new DefaultToBackendFunction(factory, metaDb, mutableMetaCollection);
            try {
                Source.fromIterator(() -> docs.iterator())
                        .grouped(docBatch)
                        .map((kvList) -> d2rFun.apply(kvList))
                        .mapConcat((collData) -> r2BackendFun.apply(collData))
                        .toMat(Sink.foreach((job) -> job.execute(backendConnection)), Keep.right()).run(materializer)
                        .toCompletableFuture()
                        .get();
            } catch (InterruptedException ex) {
                throw new SystemInterruptedException("insertion interrupted", ex);
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
