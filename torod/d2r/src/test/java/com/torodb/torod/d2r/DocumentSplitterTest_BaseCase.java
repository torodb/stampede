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

import com.torodb.torod.core.subdocument.ToroDocument;
import com.torodb.torod.core.subdocument.SubDocType;
import com.torodb.torod.core.subdocument.BasicType;
import com.torodb.torod.core.subdocument.SubDocument;
import com.torodb.torod.core.subdocument.SplitDocument;
import com.torodb.torod.core.subdocument.SubDocAttribute;
import com.torodb.torod.core.dbMetaInf.DbMetaInformationCache;
import com.torodb.torod.core.subdocument.structure.DocStructure;
import com.torodb.kvdocument.values.ObjectValue;
import com.torodb.kvdocument.converter.json.JsonValueConverter;
import java.util.*;
import javax.json.Json;
import javax.json.JsonObject;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.mockito.Mockito.*;

/**
 *
 */
public class DocumentSplitterTest_BaseCase {

    private final String collection = "test";
    private ToroDocument doc1;
    private SubDocument expectedSubDoc;
    private DbMetaInformationCache cache;
    private static final int docId = 1000;
    private static final int index = 0;
    private static final SubDocType subDocumentValueType = new SubDocType.Builder()
            .add(new SubDocAttribute("my int", BasicType.INTEGER))
            .add(new SubDocAttribute("my string", BasicType.STRING))
            .add(new SubDocAttribute("my double", BasicType.DOUBLE))
            .add(new SubDocAttribute("my long", BasicType.LONG))
            .build();
    private static final List<String> keys = Arrays.asList(new String[]{
        "my int", "my string", "my double", "my long"
    });
    private DocumentSplitter splitter;

    static {
        Collections.sort(keys);
    }

    @Before
    public void setUp() {
        JsonObject docValue = Json.createObjectBuilder()
                .add("my int", 1)
                .add("my string", "test")
                .add("my double", 3.1416d)
                .add("my long", 100230203012300l)
                .build();

        doc1 = mock(ToroDocument.class);
        when(doc1.getRoot()).thenReturn((ObjectValue) JsonValueConverter.translate(docValue));

        cache = mock(DbMetaInformationCache.class);

        expectedSubDoc = new SubDocument.Builder()
                .setDocumentId(docId)
                .setIndex(index)
                .add("my int", new com.torodb.torod.core.subdocument.values.IntegerValue(1))
                .add("my string", new com.torodb.torod.core.subdocument.values.StringValue("test"))
                .add("my double", new com.torodb.torod.core.subdocument.values.DoubleValue(3.1416d))
                .add("my long", new com.torodb.torod.core.subdocument.values.LongValue(100230203012300l))
                .build();
        splitter = new DocumentSplitter(cache);
    }

    @Test
    public void testSplit() {
        when(cache.reserveDocIds(null, collection, 1))
                .thenReturn(docId);

        SplitDocument result = splitter.split(null, collection, doc1);

        verify(cache, Mockito.times(1)).reserveDocIds(null, collection, 1);
        verifyNoMoreInteractions(cache);

        SplitDocument expected = new SplitDocument.Builder()
                .setRoot(new DocStructure.Builder()
                        .setIndex(index)
                        .setType(subDocumentValueType)
                        .built())
                .add(expectedSubDoc)
                .build();

        assert result != null;
        assert expected.equals(result);

    }

    @Test
    public void testSplit2() {
        when(cache.reserveDocIds(null, collection, 1))
                .thenReturn(docId);

        SplitDocument result = splitter.split(null, collection, doc1);

        verify(cache, Mockito.times(1)).reserveDocIds(null, collection, 1);
        verifyNoMoreInteractions(cache);

        SplitDocument expected = new SplitDocument.Builder()
                .setRoot(new DocStructure.Builder()
                        .setIndex(index)
                        .setType(subDocumentValueType)
                        .built())
                .add(expectedSubDoc)
                .build();

        assert result != null;
        assert expected.equals(result);

    }
}
