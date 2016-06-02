package com.torodb.insert.stream;

import com.torodb.core.backend.BackendConnection;
import com.torodb.core.d2r.CollectionData;
import com.torodb.core.d2r.D2RTranslator;
import com.torodb.core.dsl.backend.BackendConnectionJob;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.kvdocument.values.KVDocument;
import java.util.function.Function;
import java.util.function.Supplier;
import org.reactivestreams.Subscriber;

/**
 *
 */
public interface InsertSubscriberFactory<MDP extends MetaDocPart> {

    /**
     *
     * @param translatorSupplier   it can be stateful and must return a new instance each time
     * @param toBackendJobFunction it can be stateful
     * @param backendConnection    it can be stateful
     * @return
     * @see DefaultToBackendFunction
     */
    public Subscriber<KVDocument> createInsertSubscriber(
            Supplier<D2RTranslator> translatorSupplier,
            Function<CollectionData<MDP>, BackendConnectionJob> toBackendJobFunction,
            BackendConnection backendConnection);

}
