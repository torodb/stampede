
package com.torodb.insert.stream;

import com.torodb.core.d2r.CollectionData;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.kvdocument.values.KVDocument;
import java.util.function.Function;

/**
 *
 */
public class D2RTranslationBatchFunction<MDP extends MetaDocPart> implements Function<Iterable<KVDocument>, CollectionData<MDP>>{

    private int docBatchSize;
    private final D2RTranslatorFactory translatorFactory;

    public D2RTranslationBatchFunction(D2RTranslatorFactory translatorFactory) {
        this.translatorFactory = translatorFactory;
    }

    public int getDocBatchSize() {
        return docBatchSize;
    }

    public void setDocBatchSize(int docBatchSize) {
        this.docBatchSize = docBatchSize;
    }

    @Override
    public CollectionData<MDP> apply(Iterable<KVDocument> docs) {

        

    }


}
