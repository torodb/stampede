package com.torodb.torod.pipeline;

import com.torodb.core.d2r.D2RTranslatorFactory;
import com.torodb.core.transaction.metainf.MutableMetaCollection;
import com.torodb.core.backend.WriteBackendTransaction;

/**
 *
 */
public interface InsertPipelineFactory {

    /**
     *
     * @param translatorFactory
     * @param mutableMetaCollection
     * @param backendConnection
     * @return
     */
    public InsertPipeline createInsertSubscriber(
            D2RTranslatorFactory translatorFactory,
            MutableMetaCollection mutableMetaCollection,
            WriteBackendTransaction backendConnection);


}
