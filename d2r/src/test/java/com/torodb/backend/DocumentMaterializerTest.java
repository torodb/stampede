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

package com.torodb.backend;

import java.util.Iterator;
import java.util.LinkedHashMap;

import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.torodb.core.TableRef;
import com.torodb.core.d2r.DocPartData;
import com.torodb.core.d2r.DocPartRow;
import com.torodb.core.transaction.metainf.ImmutableMetaCollection;
import com.torodb.core.transaction.metainf.ImmutableMetaDatabase;
import com.torodb.core.transaction.metainf.ImmutableMetaSnapshot;
import com.torodb.core.transaction.metainf.MetaField;
import com.torodb.core.transaction.metainf.MetainfoRepository.SnapshotStage;
import com.torodb.core.transaction.metainf.MutableMetaSnapshot;
import com.torodb.d2r.MockRidGenerator;
import com.torodb.kvdocument.values.KVDouble;
import com.torodb.kvdocument.values.KVInteger;
import com.torodb.kvdocument.values.KVLong;
import com.torodb.kvdocument.values.KVValue;
import com.torodb.kvdocument.values.heap.ListKVArray;
import com.torodb.kvdocument.values.heap.MapKVDocument;
import com.torodb.kvdocument.values.heap.StringKVString;
import com.torodb.metainfo.cache.mvcc.MvccMetainfoRepository;

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
                        .put("f", new ListKVArray(ImmutableList.<KVValue<?>>builder()
                                .add(KVInteger.of(1))
                                .add(KVLong.of(1))
                                .add(KVDouble.of(1))
                                .add(new StringKVString("Lorem ipsum"))
                                .add(new MapKVDocument(new LinkedHashMap<>(ImmutableMap.<String, KVValue<?>>builder()
                                        .put("a", KVInteger.of(1))
                                        .put("b", KVLong.of(1))
                                        .put("c", KVDouble.of(1))
                                        .put("d", new StringKVString("Lorem ipsum"))
                                        .build())))
                                .add(new ListKVArray(ImmutableList.<KVValue<?>>builder()
                                        .add(KVInteger.of(1))
                                        .add(KVLong.of(1))
                                        .add(KVDouble.of(1))
                                        .add(new StringKVString("Lorem ipsum"))
                                        .build()))
                                .build()))
                        .build())))
                .put("f", new ListKVArray(ImmutableList.<KVValue<?>>builder()
                        .add(KVInteger.of(1))
                        .add(KVLong.of(1))
                        .add(KVDouble.of(1))
                        .add(new StringKVString("Lorem ipsum"))
                        .add(new MapKVDocument(new LinkedHashMap<>(ImmutableMap.<String, KVValue<?>>builder()
                                .put("a", KVInteger.of(1))
                                .put("b", KVLong.of(1))
                                .put("c", KVDouble.of(1))
                                .put("d", new StringKVString("Lorem ipsum"))
                                .build())))
                        .add(new ListKVArray(ImmutableList.<KVValue<?>>builder()
                                .add(KVInteger.of(1))
                                .add(KVLong.of(1))
                                .add(KVDouble.of(1))
                                .add(new StringKVString("Lorem ipsum"))
                                .build()))
                        .build()))
                .build()));
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
        
        
        D2RTranslatorImpl d2rTranslator=new D2RTranslatorImpl(new MockRidGenerator(), mutableSnapshot, "test", "test");
        
        d2rTranslator.translate(doc);
        d2rTranslator.translate(doc);
        
        
        Iterator<DocPartData> docPartDataIterator = d2rTranslator.getCollectionDataAccumulator().iterator();
        while (docPartDataIterator.hasNext()) {
            DocPartData docPartData = docPartDataIterator.next();
            TableRef tableRef = docPartData.getMetaDocPart().getTableRef();
            System.out.println("INSERT INTO " + docPartData.getMetaDocPart().getIdentifier());
            Iterator<MetaField> orderedMetaFieldIterator = docPartData.orderedMetaFieldIterator();
            if (!tableRef.getParent().isPresent()) {
                System.out.print("\t(did");
                if (orderedMetaFieldIterator.hasNext()) {
                    System.out.print(",\t\t");
                }
            } else if (!tableRef.getParent().get().getParent().isPresent()) {
                System.out.print("\t(did,\t\trid,\t\tseq");
                if (orderedMetaFieldIterator.hasNext()) {
                    System.out.print(",\t\t");
                }
            } else {
                System.out.print("\t(did,\t\trid,\t\tpid,\t\tseq");
                if (orderedMetaFieldIterator.hasNext()) {
                    System.out.print(",\t\t");
                }
            }
            while (orderedMetaFieldIterator.hasNext()) {
                MetaField metaField = orderedMetaFieldIterator.next();
                System.out.print(metaField.getIdentifier());
                if (orderedMetaFieldIterator.hasNext()) {
                    System.out.print(",\t\t");
                }
            }
            System.out.println(") VALUES ");
            Iterator<DocPartRow> docPartRowIterator = docPartData.iterator();
            while (docPartRowIterator.hasNext()) {
                DocPartRow docPartRow = docPartRowIterator.next();
                System.out.print("\t(");
                Iterator<KVValue<?>> valueIterator = docPartRow.iterator();
                if (!tableRef.getParent().isPresent()) {
                    System.out.print(docPartRow.getDid());
                    if (valueIterator.hasNext()) {
                        System.out.print(",\t\t");
                    }
                } else if (!tableRef.getParent().get().getParent().isPresent()) {
                    System.out.print(docPartRow.getDid());
                    System.out.print(",\t\t");
                    System.out.print(docPartRow.getRid());
                    System.out.print(",\t\t");
                    System.out.print(docPartRow.getSeq());
                    if (valueIterator.hasNext()) {
                        System.out.print(",\t\t");
                    }
                } else {
                    System.out.print(docPartRow.getDid());
                    System.out.print(",\t\t");
                    System.out.print(docPartRow.getRid());
                    System.out.print(",\t\t");
                    System.out.print(docPartRow.getPid());
                    System.out.print(",\t\t");
                    System.out.print(docPartRow.getSeq());
                    if (valueIterator.hasNext()) {
                        System.out.print(",\t\t");
                    }
                }
                while (valueIterator.hasNext()) {
                    System.out.print(valueIterator.next());
                    if (valueIterator.hasNext()) {
                        System.out.print(",\t\t");
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
