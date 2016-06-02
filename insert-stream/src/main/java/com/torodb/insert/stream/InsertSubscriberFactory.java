package com.torodb.insert.stream;

import com.torodb.core.backend.BackendConnection;
import com.torodb.core.d2r.CollectionData;
import com.torodb.core.dsl.backend.BackendConnectionJob;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.kvdocument.values.KVDocument;
import java.util.function.Function;
import org.reactivestreams.Subscriber;

/**
 *
 */
public interface InsertSubscriberFactory<MDP extends MetaDocPart> {

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
            Function<CollectionData, BackendConnectionJob> toBackendJobFunction,
            BackendConnection backendConnection);

}
