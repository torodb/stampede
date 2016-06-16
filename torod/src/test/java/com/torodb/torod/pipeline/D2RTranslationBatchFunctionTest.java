package com.torodb.torod.pipeline;

import com.torodb.torod.pipeline.BatchMetaCollection;
import com.torodb.torod.pipeline.D2RTranslationBatchFunction;
import com.torodb.core.d2r.D2RTranslatorFactory;
import com.google.common.collect.Lists;
import com.torodb.core.d2r.CollectionData;
import com.torodb.core.d2r.D2RTranslator;
import com.torodb.core.transaction.metainf.ImmutableMetaCollection;
import com.torodb.core.transaction.metainf.MutableMetaCollection;
import com.torodb.core.transaction.metainf.WrapperMutableMetaCollection;
import com.torodb.kvdocument.values.KVDocument;
import java.util.Collections;
import java.util.List;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.*;

/**
 *
 * @author gortiz
 */
public class D2RTranslationBatchFunctionTest {

    private D2RTranslationBatchFunction fun;
    private D2RTranslatorFactory translatorFactory;
    private WrapperMutableMetaCollection metaCol;
    private BatchMetaCollection batchMetaCol;


    public D2RTranslationBatchFunctionTest() {
    }

    @Before
    public void setUp() {
        metaCol = new WrapperMutableMetaCollection(
                new ImmutableMetaCollection("colName", "colId", Collections.emptyMap()),
                (o) -> {});

        translatorFactory = mock(D2RTranslatorFactory.class);

        fun = new D2RTranslationBatchFunction(translatorFactory, metaCol) {
            @Override
            protected BatchMetaCollection createMetaDocCollection(MutableMetaCollection metaCol) {
                batchMetaCol = spy(super.createMetaDocCollection(metaCol));
                return batchMetaCol;
            }

        };
    }

    @Test
    public void testApply() {
        D2RTranslator translator = mock(D2RTranslator.class);
        CollectionData colData = mock(CollectionData.class);
        KVDocument doc1 = mock(KVDocument.class);
        KVDocument doc2 = mock(KVDocument.class);

        given(translator.getCollectionDataAccumulator())
                .willReturn(colData);

        given(translatorFactory.createTranslator(batchMetaCol))
                .willReturn(translator);

        List<KVDocument> docs = Lists.newArrayList(doc1, doc2);

        //when
        CollectionData result = fun.apply(docs);

        //then
        verify(batchMetaCol).newBatch();
        verify(translator).translate(doc1);
        verify(translator).translate(doc2);
        verify(translator).getCollectionDataAccumulator();
        verifyNoMoreInteractions(translator);

        verify(translatorFactory)
                .createTranslator(batchMetaCol);
        verifyNoMoreInteractions(translatorFactory);
        assertEquals(colData, result);
    }

}
