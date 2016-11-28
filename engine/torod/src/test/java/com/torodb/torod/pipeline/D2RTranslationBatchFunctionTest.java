/*
 * ToroDB
 * Copyright © 2014 8Kdata Technology (www.8kdata.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.torodb.torod.pipeline;

import static org.junit.Assert.assertEquals;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.*;

import com.google.common.collect.Lists;
import com.torodb.core.d2r.CollectionData;
import com.torodb.core.d2r.D2RTranslator;
import com.torodb.core.d2r.D2RTranslatorFactory;
import com.torodb.core.transaction.metainf.ImmutableMetaDatabase;
import com.torodb.core.transaction.metainf.MutableMetaCollection;
import com.torodb.core.transaction.metainf.WrapperMutableMetaCollection;
import com.torodb.core.transaction.metainf.WrapperMutableMetaDatabase;
import com.torodb.kvdocument.values.KvDocument;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

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
    metaDb = new WrapperMutableMetaDatabase(new ImmutableMetaDatabase("dbName", "dbId", Collections
        .emptyList()), (o) -> {
    });
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
    KvDocument doc1 = mock(KvDocument.class);
    KvDocument doc2 = mock(KvDocument.class);

    given(translator.getCollectionDataAccumulator())
        .willReturn(colData);

    given(translatorFactory.createTranslator(metaDb, batchMetaCol))
        .willReturn(translator);

    List<KvDocument> docs = Lists.newArrayList(doc1, doc2);

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
