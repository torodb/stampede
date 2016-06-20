package com.torodb.torod.pipeline;

import com.google.common.collect.Lists;
import com.torodb.core.d2r.CollectionData;
import com.torodb.core.d2r.D2RTranslator;
import com.torodb.core.d2r.D2RTranslatorFactory;
import com.torodb.core.transaction.metainf.ImmutableMetaDatabase;
import com.torodb.core.transaction.metainf.MutableMetaCollection;
import com.torodb.core.transaction.metainf.WrapperMutableMetaCollection;
import com.torodb.core.transaction.metainf.WrapperMutableMetaDatabase;
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
    private WrapperMutableMetaDatabase metaDb;
    private WrapperMutableMetaCollection metaCol;
    private BatchMetaCollection batchMetaCol;


    public D2RTranslationBatchFunctionTest() {
    }

    @Before
    public void setUp() {
        metaDb = new WrapperMutableMetaDatabase(new ImmutableMetaDatabase("dbName", "dbId", Collections.emptyList()), (o) -> {});
        metaCol = metaDb.addMetaCollection("colName", "colId");

        translatorFactory = mock(D2RTranslatorFactory.class);

        fun = new D2RTranslationBatchFunction(translatorFactory, metaDb, metaCol) {
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

        given(translatorFactory.createTranslator(metaDb, batchMetaCol))
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
                .createTranslator(metaDb, batchMetaCol);
        verifyNoMoreInteractions(translatorFactory);
        assertEquals(colData, result);
    }

}
