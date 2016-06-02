
package com.torodb.insert.stream;

import com.torodb.core.d2r.CollectionData;
import com.torodb.core.d2r.D2RTranslator;
import com.torodb.core.transaction.metainf.MutableMetaCollection;
import com.torodb.kvdocument.values.KVDocument;
import java.util.function.Function;

/**
 *
 */
public class D2RTranslationBatchFunction implements Function<Iterable<KVDocument>, CollectionData>{

    private int docBatchSize;
    private final D2RTranslatorFactory translatorFactory;
    private final BatchMetaCollection metaDocCollection;

    public D2RTranslationBatchFunction(D2RTranslatorFactory translatorFactory,
            MutableMetaCollection metaCol) {
        this.translatorFactory = translatorFactory;
        this.metaDocCollection = new BatchMetaCollection(metaCol);
    }

    public int getDocBatchSize() {
        return docBatchSize;
    }

    public void setDocBatchSize(int docBatchSize) {
        this.docBatchSize = docBatchSize;
    }

    @Override
    public CollectionData apply(Iterable<KVDocument> docs) {
        metaDocCollection.newBatch();
        D2RTranslator translator = translatorFactory.createTranslator(metaDocCollection);

        for (KVDocument doc : docs) {
            translator.translate(doc);
        }

        return translator.getCollectionDataAccumulator();
    }


}
