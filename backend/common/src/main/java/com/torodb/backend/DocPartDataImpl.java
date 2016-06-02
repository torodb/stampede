package com.torodb.backend;

import java.util.ArrayList;
import java.util.Iterator;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.torodb.core.d2r.DocPartData;
import com.torodb.core.d2r.DocPartRow;
import com.torodb.core.transaction.metainf.FieldType;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.core.transaction.metainf.MetaField;
import com.torodb.core.transaction.metainf.MutableMetaDocPart;
import com.torodb.kvdocument.values.KVValue;

public class DocPartDataImpl extends ArrayList<DocPartRow> implements DocPartData {
        private static final long serialVersionUID = 1L;
        
        private final MutableMetaDocPart metaDocPart;
        private final DocPartRidGenerator docPartRidGenerator;
        private final BiMap<KeyFieldType, Integer> columnIndexMap = HashBiMap.create();
        int currentIndex = 0;
        
        public DocPartDataImpl(MutableMetaDocPart metaDocPart, DocPartRidGenerator docPartRidGenerator) {
            super();
            this.metaDocPart = metaDocPart;
            this.docPartRidGenerator = docPartRidGenerator;
        }
        
        public MutableMetaDocPart getMutableMetaDocPart() {
            return metaDocPart;
        }
        
        @Override
        public MetaDocPart getMetaDocPart() {
            return metaDocPart;
        }
        
        @Override
        public int columnCount() {
            return columnIndexMap.size();
        }
        
        @Override
        public int rowCount() {
            return size();
        }
        
        @Override
        public Iterator<MetaField> orderedMetaFieldIterator() {
            return new Iterator<MetaField>() {
                private final int count = columnCount();
                private int index = 0;
                
                @Override
                public boolean hasNext() {
                    return index < count;
                }
                
                @Override
                public MetaField next() {
                    KeyFieldType keyFieldType = columnIndexMap.inverse().get(index++);
                    return metaDocPart.getMetaFieldByNameAndType(keyFieldType.getKey(), keyFieldType.getFieldType());
                }
            };
        }
        
        public DocPartRowImpl appendRootRow() {
            return appendRow(new DocPartRowImpl(this, docPartRidGenerator.nextRid()));
        }
        
        public DocPartRowImpl appendObjectRow(int did) {
            return appendRow(new DocPartRowImpl(this, did, docPartRidGenerator.nextRid()));
        }
        
        public DocPartRowImpl appendObjectRow(int did, int pid) {
            return appendRow(new DocPartRowImpl(this, did, docPartRidGenerator.nextRid(), pid));
        }
        
        public DocPartRowImpl appendArrayRow(int did, int pid, int seq) {
            return appendRow(new DocPartRowImpl(this, did, docPartRidGenerator.nextRid(), pid, seq));
        }

        private DocPartRowImpl appendRow(DocPartRowImpl row) {
            add(row);
            return row;
        }
        
        public void appendColumnValue(DocPartRowImpl row, String key, String identifier, FieldType fieldType, KVValue<?> value) {
            KeyFieldType keyFieldType = new KeyFieldType(key, fieldType);
            Integer index = columnIndexMap.get(keyFieldType);
            if (index == null) {
                index = currentIndex++;
                columnIndexMap.put(keyFieldType, index);
            }
            row.appendColumnValue(identifier, value, index);
        }
    }
    
    