package com.torodb.insert.stream;

import com.torodb.core.backend.BackendConnection;
import com.torodb.core.transaction.metainf.MutableMetaCollection;
import com.torodb.kvdocument.values.KVDocument;
import org.reactivestreams.Subscriber;

/**
 *
 */
public interface InsertSubscriberFactory {

    /**
     *
     * @param translatorFactory
     * @param toBackendJobFunction
     * @param backendConnection
     * @return
     * @see DefaultToBackendFunction
     */
    public Subscriber<KVDocument> createInsertSubscriber(
            D2RTranslatorFactory translatorFactory,
            MutableMetaCollection mutableMetaCollection,
            BackendConnection backendConnection);

}
