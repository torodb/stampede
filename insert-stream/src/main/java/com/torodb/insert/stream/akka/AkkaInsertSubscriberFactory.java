
package com.torodb.insert.stream.akka;

import akka.NotUsed;
import akka.stream.Materializer;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import com.torodb.core.backend.BackendConnection;
import com.torodb.core.dsl.backend.BackendConnectionJobFactory;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MutableMetaCollection;
import com.torodb.insert.stream.D2RTranslationBatchFunction;
import com.torodb.insert.stream.D2RTranslatorFactory;
import com.torodb.insert.stream.DefaultToBackendFunction;
import com.torodb.insert.stream.InsertSubscriberFactory;
import com.torodb.kvdocument.values.KVDocument;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import org.reactivestreams.Subscriber;
import scala.concurrent.duration.FiniteDuration;

/**
 *
 */
public class AkkaInsertSubscriberFactory implements InsertSubscriberFactory {

    private final Materializer materializer;
    private final BackendConnectionJobFactory factory;
    private final MetaDatabase database;

    @Inject
    public AkkaInsertSubscriberFactory(Materializer materializer,
            BackendConnectionJobFactory factory, MetaDatabase database) {
        this.materializer = materializer;
        this.factory = factory;
        this.database = database;
    }

    @Override
    public Subscriber<KVDocument> createInsertSubscriber(
            D2RTranslatorFactory translatorFactory,
            MutableMetaCollection mutableMetaCollection,
            BackendConnection backendConnection) {

        D2RTranslationBatchFunction d2rFun = new D2RTranslationBatchFunction(translatorFactory, mutableMetaCollection);
        DefaultToBackendFunction r2BackendFun = new DefaultToBackendFunction(factory, database, mutableMetaCollection);

        Sink<KVDocument, NotUsed> sink = Flow.of(KVDocument.class)
                .groupedWithin(100, FiniteDuration.apply(30, TimeUnit.MILLISECONDS))
                .map((kvList) -> d2rFun.apply(kvList))
                .mapConcat((collData) -> r2BackendFun.apply(collData))
                .to(Sink.foreach((job) -> job.execute(backendConnection)));

        return sink.runWith(Source.asSubscriber(), materializer);
    }

}
