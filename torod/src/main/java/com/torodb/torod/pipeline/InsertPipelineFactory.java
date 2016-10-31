package com.torodb.torod.pipeline;

import com.google.common.util.concurrent.Service;
import com.torodb.core.backend.WriteBackendTransaction;
import com.torodb.core.d2r.D2RTranslatorFactory;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MutableMetaCollection;

/**
 *
 */
public interface InsertPipelineFactory extends Service {

    /**
     *
     * @param translatorFactory
     * @param metaDb
     * @param mutableMetaCollection
     * @param backendConnection
     * @return
     */
    public InsertPipeline createInsertPipeline(
            D2RTranslatorFactory translatorFactory,
            MetaDatabase metaDb,
            MutableMetaCollection mutableMetaCollection,
            WriteBackendTransaction backendConnection,
            boolean concurrent);


}
