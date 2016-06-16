
package com.torodb.torod.pipeline;

import com.torodb.core.d2r.D2RTranslatorFactory;
import com.torodb.core.d2r.CollectionData;
import com.torodb.core.d2r.D2RTranslator;
import com.torodb.core.transaction.metainf.MutableMetaCollection;
import com.torodb.kvdocument.values.KVDocument;
import java.util.List;
import java.util.function.Function;

/**
 *
 */
public class D2RTranslationBatchFunction implements Function<List<KVDocument>, CollectionData>{

    private final D2RTranslatorFactory translatorFactory;
    private final BatchMetaCollection metaDocCollection;

    public D2RTranslationBatchFunction(D2RTranslatorFactory translatorFactory,
            MutableMetaCollection metaCol) {
        this.translatorFactory = translatorFactory;
        this.metaDocCollection = createMetaDocCollection(metaCol);
    }

    //For testing purpose
    protected BatchMetaCollection createMetaDocCollection(MutableMetaCollection metaCol) {
        return new BatchMetaCollection(metaCol);
    }

    @Override
    public CollectionData apply(List<KVDocument> docs) {
        metaDocCollection.newBatch();
        D2RTranslator translator = translatorFactory.createTranslator(metaDocCollection);

        for (KVDocument doc : docs) {
            translator.translate(doc);
        }

        return translator.getCollectionDataAccumulator();
    }


}
