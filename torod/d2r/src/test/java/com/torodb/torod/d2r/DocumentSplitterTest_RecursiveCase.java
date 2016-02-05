/*
 *     This file is part of ToroDB.
 *
 *     ToroDB is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Affero General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     ToroDB is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Affero General Public License for more details.
 *
 *     You should have received a copy of the GNU Affero General Public License
 *     along with ToroDB. If not, see <http://www.gnu.org/licenses/>.
 *
 *     Copyright (c) 2014, 8Kdata Technology
 *     
 */

package com.torodb.torod.d2r;


import com.eightkdata.mongowp.bson.BsonDocument;
import com.eightkdata.mongowp.utils.BsonDocumentBuilder;
import com.torodb.kvdocument.conversion.mongowp.MongoWPConverter;
import com.torodb.kvdocument.values.KVDocument;
import com.torodb.torod.core.dbMetaInf.DbMetaInformationCache;
import com.torodb.torod.core.subdocument.*;
import com.torodb.torod.core.subdocument.structure.DocStructure;
import com.torodb.torod.core.subdocument.values.ScalarDouble;
import com.torodb.torod.core.subdocument.values.ScalarInteger;
import com.torodb.torod.core.subdocument.values.ScalarLong;
import com.torodb.torod.core.subdocument.values.heap.StringScalarString;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static com.eightkdata.mongowp.bson.utils.DefaultBsonValues.*;
import static org.mockito.Mockito.*;

/**
 *
 */
public class DocumentSplitterTest_RecursiveCase {

    private final String collection = "test";
    private ToroDocument doc1;
    private SubDocument expectedSubDoc;
    private SubDocument embeddedSubDoc;
    private DbMetaInformationCache cache;
    private static final int docId1 = 1000;
    private static final int index1 = 0;
    private static final int docId2 = 32;
    private static final int index2 = 0;
    private static final SimpleSubDocTypeBuilderProvider SSD_BUILLDER_PROVIDER = new SimpleSubDocTypeBuilderProvider();
    private static final SubDocType subDocumentValueType1 = SSD_BUILLDER_PROVIDER.get()
            .add(new SubDocAttribute("my int",ScalarType.INTEGER))
            .add(new SubDocAttribute("my string", ScalarType.STRING))
            .add(new SubDocAttribute("my double", ScalarType.DOUBLE))
            .add(new SubDocAttribute("my long", ScalarType.LONG))
            .build();
    private static final SubDocType subDocumentValueType2 = SSD_BUILLDER_PROVIDER.get()
            .build();
    private static final List<String> keys1 = Arrays.asList(new String[]{
        "my int", "my string", "my double", "my long", "my embedded"
    });
    private static final List<String> keys2 = Arrays.asList(new String[]{});
    private DocumentSplitter splitter;

    static {
        Collections.sort(keys1);
    }

    @Before
    public void setUp() {
        BsonDocument docValue = new BsonDocumentBuilder()
                .appendUnsafe("my int", newInt(1))
                .appendUnsafe("my string", newString("test"))
                .appendUnsafe("my double", newDouble(3.1416d))
                .appendUnsafe("my long", newLong(100230203012300l))
                .appendUnsafe("my embedded", EMPTY_DOC)
                .build();

        doc1 = mock(ToroDocument.class);
        when(doc1.getRoot()).thenReturn((KVDocument) MongoWPConverter.translate(docValue));

        cache = mock(DbMetaInformationCache.class);

        expectedSubDoc = SubDocument.Builder.withUnknownType(SSD_BUILLDER_PROVIDER)
                .setDocumentId(docId1)
                .add("my int", ScalarInteger.of(1))
                .add("my string", new StringScalarString("test"))
                .add("my double", ScalarDouble.of(3.1416d))
                .add("my long", ScalarLong.of(100230203012300l))
                .build();
        embeddedSubDoc = SubDocument.Builder.withUnknownType(SSD_BUILLDER_PROVIDER)
                .setDocumentId(docId2)
                .build();
        splitter = new DocumentSplitter(cache, SSD_BUILLDER_PROVIDER);
    }

    @Test
    public void testSplit() {
        when(cache.reserveDocIds(null, collection, 1)).thenReturn(docId1);
        when(cache.reserveDocIds(null, collection, 1)).thenReturn(docId2);

        SplitDocument result = splitter.split(null, collection, doc1);

        verify(cache, Mockito.times(1)).reserveDocIds(null, collection, 1);
        verify(cache, Mockito.times(1)).reserveDocIds(null, collection, 1);
        verifyNoMoreInteractions(cache);

        SplitDocument expected = new SplitDocument.Builder()
                .setRoot(new DocStructure.Builder()
                        .setIndex(index1)
                        .setType(subDocumentValueType1)
                        .add("my embedded", new DocStructure.Builder()
                                .setIndex(index2)
                                .setType(subDocumentValueType2)
                                .built()
                        )
                        .built())
                .add(expectedSubDoc)
                .add(embeddedSubDoc)
                .build();

        assert result != null;
        assert expected.equals(result);

    }

    @Test
    public void testSplit2() {
        when(cache.reserveDocIds(null, collection, 1)).thenReturn(docId1);
        when(cache.reserveDocIds(null, collection, 1)).thenReturn(docId2);

        SplitDocument result = splitter.split(null, collection, doc1);

        verify(cache, Mockito.times(1)).reserveDocIds(null, collection, 1);
        verify(cache, Mockito.times(1)).reserveDocIds(null, collection, 1);
        verifyNoMoreInteractions(cache);

        SplitDocument expected = new SplitDocument.Builder()
                .setRoot(new DocStructure.Builder()
                        .setIndex(index1)
                        .setType(subDocumentValueType1)
                        .add("my embedded", new DocStructure.Builder()
                                .setIndex(index2)
                                .setType(subDocumentValueType2)
                                .built()
                        )
                        .built())
                .add(expectedSubDoc)
                .add(embeddedSubDoc)
                .build();

        assert result != null;
        assert expected.equals(result);
    }
}
