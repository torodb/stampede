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

package com.torodb.poc.backend;

import java.util.Iterator;
import java.util.LinkedHashMap;

import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.torodb.core.transaction.metainf.ImmutableMetaCollection;
import com.torodb.core.transaction.metainf.ImmutableMetaDatabase;
import com.torodb.core.transaction.metainf.ImmutableMetaSnapshot;
import com.torodb.core.transaction.metainf.MetainfoRepository.SnapshotStage;
import com.torodb.core.transaction.metainf.MutableMetaCollection;
import com.torodb.core.transaction.metainf.MutableMetaSnapshot;
import com.torodb.kvdocument.values.KVDouble;
import com.torodb.kvdocument.values.KVInteger;
import com.torodb.kvdocument.values.KVLong;
import com.torodb.kvdocument.values.KVValue;
import com.torodb.kvdocument.values.heap.ListKVArray;
import com.torodb.kvdocument.values.heap.MapKVDocument;
import com.torodb.kvdocument.values.heap.StringKVString;
import com.torodb.metainfo.cache.mvcc.MvccMetainfoRepository;
import com.torodb.poc.backend.DocumentMaterializerVisitor.CollectionMaterializer;
import com.torodb.poc.backend.DocumentMaterializerVisitor.DocPartData;
import com.torodb.poc.backend.DocumentMaterializerVisitor.DocPartMaterializer;
import com.torodb.poc.backend.DocumentMaterializerVisitor.DocPartRow;
import com.torodb.poc.backend.DocumentMaterializerVisitor.KeyFieldType;

public class DocumentMaterializerTest {
    @Test
    public void testSimple() throws Exception {
        MapKVDocument doc = new MapKVDocument(new LinkedHashMap<>(ImmutableMap.<String, KVValue<?>>builder()
                .put("a", KVInteger.of(1))
                .put("b", KVLong.of(1))
                .put("c", KVDouble.of(1))
                .put("d", new StringKVString("Lorem ipsum"))
                .put("e", new MapKVDocument(new LinkedHashMap<>(ImmutableMap.<String, KVValue<?>>builder()
                        .put("a", KVInteger.of(1))
                        .put("b", KVLong.of(1))
                        .put("c", KVDouble.of(1))
                        .put("d", new StringKVString("Lorem ipsum"))
                        .build())))
                .put("f", new ListKVArray(ImmutableList.<KVValue<?>>builder()
                        .add(KVInteger.of(1))
                        .add(KVLong.of(1))
                        .add(KVDouble.of(1))
                        .add(new StringKVString("Lorem ipsum"))
                        .build()))
                .build()));
        DocumentMaterializerVisitor documentMaterializerVisitor = new DocumentMaterializerVisitor();
        ImmutableMetaSnapshot currentView = new ImmutableMetaSnapshot.Builder()
                .add(new ImmutableMetaDatabase.Builder("test", "test")
                        .add(new ImmutableMetaCollection.Builder("test", "test")
                                .build())
                        .build())
                .build();
        MvccMetainfoRepository mvccMetainfoRepository = new MvccMetainfoRepository(currentView);
        MutableMetaSnapshot mutableSnapshot;
        try (SnapshotStage snapshot = mvccMetainfoRepository.startSnapshotStage()) {
            mutableSnapshot = snapshot.createMutableSnapshot();
        }
        
        MutableMetaCollection mutableMetaCollection = 
                mutableSnapshot.getMetaDatabaseByName("test").getMetaCollectionByName("test");
        CollectionMaterializer collectionMaterializer = documentMaterializerVisitor.getCollectionMaterializer(mutableMetaCollection);
        
        doc.accept(documentMaterializerVisitor, collectionMaterializer);
        
        Iterator<DocPartMaterializer> docPartMaterializerIterator = collectionMaterializer.getRootPartMaterializer().getDocPartMaterializer().docPartMaterializerIterator();
        while (docPartMaterializerIterator.hasNext()) {
            DocPartMaterializer docPartMaterializer = docPartMaterializerIterator.next();
            DocPartData docPartData = docPartMaterializer.getDocPartData();
            System.out.println("INSERT INTO " + docPartMaterializer.getMetaDocPart().getIdentifier());
            Iterator<KeyFieldType> orderedKeyTypeIdIterator = docPartData.orderedKeyFieldTypeIterator();
            if (docPartMaterializer.isRoot()) {
                System.out.print("\t(did");
                if (orderedKeyTypeIdIterator.hasNext()) {
                    System.out.print(", ");
                }
            } else if (docPartMaterializer.getParentDocPartMaterializer().isRoot()) {
                System.out.print("\t(did, rid, seq");
                if (orderedKeyTypeIdIterator.hasNext()) {
                    System.out.print(", ");
                }
            } else {
                System.out.print("\t(did, rid, pid, seq");
                if (orderedKeyTypeIdIterator.hasNext()) {
                    System.out.print(", ");
                }
            }
            while (orderedKeyTypeIdIterator.hasNext()) {
                KeyFieldType keyFieldType = orderedKeyTypeIdIterator.next();
                System.out.print(keyFieldType);
                if (orderedKeyTypeIdIterator.hasNext()) {
                    System.out.print(", ");
                }
            }
            System.out.println(") VALUES ");
            Iterator<DocPartRow> docPartRowIterator = docPartData.iterator();
            while (docPartRowIterator.hasNext()) {
                DocPartRow docPartRow = docPartRowIterator.next();
                System.out.print("\t(");
                Iterator<String> valueIterator = docPartRow.iterator();
                if (docPartMaterializer.isRoot()) {
                    System.out.print(docPartRow.getDid());
                    if (valueIterator.hasNext()) {
                        System.out.print(", ");
                    }
                } else if (docPartMaterializer.getParentDocPartMaterializer().isRoot()) {
                    System.out.print(docPartRow.getDid());
                    System.out.print(", ");
                    System.out.print(docPartRow.getRid());
                    System.out.print(", ");
                    System.out.print(docPartRow.getSeq());
                    if (valueIterator.hasNext()) {
                        System.out.print(", ");
                    }
                } else {
                    System.out.print(docPartRow.getDid());
                    System.out.print(", ");
                    System.out.print(docPartRow.getRid());
                    System.out.print(", ");
                    System.out.print(docPartRow.getPid());
                    System.out.print(", ");
                    System.out.print(docPartRow.getSeq());
                    if (valueIterator.hasNext()) {
                        System.out.print(", ");
                    }
                }
                while (valueIterator.hasNext()) {
                    System.out.print(valueIterator.next());
                    if (valueIterator.hasNext()) {
                        System.out.print(", ");
                    }
                }
                System.out.print(")");
                if (docPartRowIterator.hasNext()) {
                    System.out.println(",");
                }
            }
            System.out.println();
        }
    }
}
