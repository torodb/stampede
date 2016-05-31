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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import javax.annotation.Nonnull;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.torodb.core.TableRef;
import com.torodb.core.impl.TableRefImpl;
import com.torodb.core.transaction.metainf.FieldType;
import com.torodb.core.transaction.metainf.MetaField;
import com.torodb.core.transaction.metainf.MutableMetaCollection;
import com.torodb.core.transaction.metainf.MutableMetaDocPart;
import com.torodb.kvdocument.values.KVArray;
import com.torodb.kvdocument.values.KVBinary;
import com.torodb.kvdocument.values.KVBoolean;
import com.torodb.kvdocument.values.KVDate;
import com.torodb.kvdocument.values.KVDocument;
import com.torodb.kvdocument.values.KVDocument.DocEntry;
import com.torodb.kvdocument.values.KVDouble;
import com.torodb.kvdocument.values.KVInstant;
import com.torodb.kvdocument.values.KVInteger;
import com.torodb.kvdocument.values.KVLong;
import com.torodb.kvdocument.values.KVMongoObjectId;
import com.torodb.kvdocument.values.KVMongoTimestamp;
import com.torodb.kvdocument.values.KVNull;
import com.torodb.kvdocument.values.KVString;
import com.torodb.kvdocument.values.KVTime;
import com.torodb.kvdocument.values.KVValue;
import com.torodb.kvdocument.values.KVValueVisitor;
import com.torodb.poc.backend.mocks.ToroImplementationException;

public class DocumentMaterializerVisitor implements KVValueVisitor<Void, DocumentMaterializerVisitor.Materializer> {

    private final static Map<DocPartMaterializer, Integer> lastRidMap =
            Maps.newHashMap();
    private static final AppendValueVisitor objectAppendValueVisitor = new ObjectAppendValueVisitor();
    private static final AppendValueVisitor arrayAppendValueVisitor = new ArrayAppendValueVisitor();
    
    private final Map<String, CollectionMaterializer> collectionMaterializerMap = Maps.newHashMap();
    
    @Override
    public Void visit(KVInteger value, Materializer arg) {
        return null;
    }

    @Override
    public Void visit(KVLong value, Materializer arg) {
        return null;
    }

    @Override
    public Void visit(KVString value, Materializer arg) {
        return null;
    }
    @Override
    public Void visit(KVDouble value, Materializer arg) {
        return null;
    }

    @Override
    public Void visit(KVBoolean value, Materializer arg) {
        return null;
    }

    @Override
    public Void visit(KVNull value, Materializer arg) {
        return null;
    }

    @Override
    public Void visit(KVMongoObjectId value, Materializer arg) {
        return null;
    }

    @Override
    public Void visit(KVInstant value, Materializer arg) {
        return null;
    }

    @Override
    public Void visit(KVDate value, Materializer arg) {
        return null;
    }

    @Override
    public Void visit(KVTime value, Materializer arg) {
        return null;
    }
    
    @Override
    public Void visit(KVBinary value, Materializer arg) {
        return null;
    }

    @Override
    public Void visit(KVMongoTimestamp value, Materializer arg) {
        return null;
    }
    
    @Override
    public Void visit(KVDocument value, Materializer arg) {
        arg = arg.beginAppendObject();
        
        for (DocEntry<?> entry : value) {
            entry.getValue().accept(this, arg.appendValueFromObject(entry.getKey(), entry.getValue()));
        }
        
        arg.endAppendObject();
        
        return null;
    }
    
    @Override
    public Void visit(KVArray value, Materializer arg) {
        arg = arg.beginAppendArray();
        
        int index = 0;
        for (KVValue<?> element : value) {
            arg.appendArrayElement(index++);
            element.accept(this, arg.appendValueFromArray(element));
        }
        
        arg.endAppendArray();
        
        return null;
    }

    public CollectionMaterializer getCollectionMaterializer(MutableMetaCollection metaCollection) {
        CollectionMaterializer collectionMaterializer = collectionMaterializerMap.get(metaCollection.getName());
        if (collectionMaterializer == null) {
            collectionMaterializer = new CollectionMaterializer(metaCollection);
            collectionMaterializerMap.put(metaCollection.getName(), collectionMaterializer);
        }
        return collectionMaterializer;
    }

    //object (dimension == 0) and array of dimension 1 share same table
    public class KeyDimension {
        public final String key;
        //dimension = 0 is object, dimension > 0 is array
        public final int dimension;
        public final String identifier;
        
        public KeyDimension(@Nonnull String key) {
            super();
            this.key = key;
            this.dimension = 0;
            this.identifier = key;
        }
        
        public KeyDimension incrementDimension() {
            return new KeyDimension(key, dimension+1);
        }
        
        private KeyDimension(@Nonnull String key, int dimension) {
            this.key = key;
            this.dimension = dimension;
            this.identifier = dimension < 2 ? key : key + '$' + dimension;
        }
        
        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + key.hashCode();
            result = prime * result + dimension;
            return result;
        }
        
        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            KeyDimension other = (KeyDimension) obj;
            if (key == null) {
                if (other.key != null)
                    return false;
            } else if (!key.equals(other.key))
                return false;
            if (dimension != other.dimension 
                    && !(dimension == 0 && other.dimension == 1)
                    && !(dimension == 1 && other.dimension == 0))
                return false;
            return true;
        }
        
        public String getIdentifier() {
            return identifier;
        }

        @Override
        public String toString() {
            return key + ':' + dimension + " <" + identifier + '>';
        }
    }
    
    public interface Materializer {
        public Materializer beginAppendObject();
        public void endAppendObject();
        public Materializer beginAppendArray();
        public void endAppendArray();
        public Materializer appendArrayElement(int index);
        public Materializer appendValueFromObject(String key, KVValue<?> value);
        public Materializer appendValueFromArray(KVValue<?> value);
        public void appendScalarValue(KVValue<?> value);
        public void appendKeyValue(KVValue<?> value);
    }
    
    public class CollectionMaterializer implements Materializer {
        private final PartMaterializer rootPartMaterializer;
        
        public CollectionMaterializer(MutableMetaCollection metaCollection) {
            super();
            TableRef rootTableRef = TableRefImpl.createRoot();
            KeyDimension keyDimension = new KeyDimension(metaCollection.getName());
            this.rootPartMaterializer = new PartMaterializer(keyDimension, 
                    new DocPartMaterializer(metaCollection, rootTableRef, null, null, keyDimension));
        }
        
        public PartMaterializer getRootPartMaterializer() {
            return rootPartMaterializer;
        }

        public Materializer beginAppendObject() {
            rootPartMaterializer.beginAppendRootObject();
            return rootPartMaterializer;
        }
        
        public void endAppendObject() {
            rootPartMaterializer.endAppendObject();
        }
        
        public Materializer beginAppendArray() {
            throw new ToroImplementationException("Cannot begin append array on root materializer");
        }
        
        public void endAppendArray() {
            throw new ToroImplementationException("Cannot end append array on root materializer");
        }
        
        public Materializer appendArrayElement(int index) {
            throw new ToroImplementationException("Cannot append array element on root materializer");
        }
        
        public Materializer appendValueFromObject(String key, KVValue<?> value) {
            throw new ToroImplementationException("Cannot append value root materializer");
        }
        
        public Materializer appendValueFromArray(KVValue<?> value) {
            throw new ToroImplementationException("Cannot append value on root materializer");
        }
        
        public void appendScalarValue(KVValue<?> value) {
            throw new ToroImplementationException("Cannot append value on root materializer");
        }
        
        public void appendKeyValue(KVValue<?> value) {
            throw new ToroImplementationException("Cannot append value on root materializer");
        }
    }
    
    public class PartMaterializer implements Materializer {
        private final KeyDimension keyDimension;
        private final DocPartMaterializer docPartMaterializer;
        
        public PartMaterializer(KeyDimension keyDimension, DocPartMaterializer docPartMaterializer) {
            super();
            this.keyDimension = keyDimension;
            this.docPartMaterializer = docPartMaterializer;
        }

        public KeyDimension getKeyDimension() {
            return keyDimension;
        }
        
        public DocPartMaterializer getDocPartMaterializer() {
            return docPartMaterializer;
        }
        
        public Materializer beginAppendRootObject() {
            docPartMaterializer.beginAppendRootObject();
            return this;
        }
        
        public Materializer beginAppendObject() {
            return new PartMaterializer(keyDimension, docPartMaterializer.beginAppendObject(keyDimension));
        }
        
        public void endAppendObject() {
            docPartMaterializer.endAppendObject();
        }
        
        public Materializer beginAppendArray() {
            KeyDimension keyDimension = this.keyDimension.incrementDimension();
            return new PartMaterializer(keyDimension, docPartMaterializer.beginAppendArray(keyDimension));
        }
        
        public void endAppendArray() {
            docPartMaterializer.endAppendArray();
        }
        
        public Materializer appendArrayElement(int index) {
            docPartMaterializer.appendElement(index);
            return this;
        }
        
        public Materializer appendValueFromObject(String key, KVValue<?> value) {
            PartMaterializer partMaterializer = new PartMaterializer(new KeyDimension(key), docPartMaterializer);
            value.accept(objectAppendValueVisitor, partMaterializer);
            return partMaterializer;
        }
        
        public Materializer appendValueFromArray(KVValue<?> value) {
            value.accept(arrayAppendValueVisitor, this);
            return this;
        }
        
        public void appendKeyValue(KVValue<?> value) {
            docPartMaterializer.appendKeyValue(keyDimension.key, value);
        }
        
        public void appendScalarValue(KVValue<?> value) {
            docPartMaterializer.appendScalarValue(value);
        }
    }
    
    public class DocPartMaterializer {
        private final MutableMetaCollection metaCollection;
        private final MutableMetaDocPart metaDocPart;
        private final TableRef tableRef;
        private final DocPartMaterializer parentDocPartMaterializer;
        private final DocPartMaterializer keyDimensionParentDocPartMaterializer;
        private final int level;
        private final KeyDimension keyDimension;
        private final DocPartData docPartData;
        private final DocPartData rootDocPartData;
        private final Map<KeyDimension, DocPartMaterializer> childMap = Maps.newHashMap();
        
        private DocPartMaterializer(MutableMetaCollection metaCollection, TableRef tableRef, DocPartMaterializer parentDocPartMaterializer, DocPartMaterializer keyDimensionParentDocPartMaterializer, KeyDimension keyDimension) {
            super();
            this.metaCollection = metaCollection;
            this.tableRef = tableRef;
            this.parentDocPartMaterializer = parentDocPartMaterializer;
            this.keyDimensionParentDocPartMaterializer = keyDimensionParentDocPartMaterializer == null ? this : keyDimensionParentDocPartMaterializer;
            MutableMetaDocPart metaDocPart = metaCollection.getMetaDocPartByTableRef(tableRef);
            if (metaDocPart == null) {
                String identifier = generateTableName(parentDocPartMaterializer, keyDimension);
                metaDocPart = metaCollection.addMetaDocPart(tableRef, identifier);
            }
            this.metaDocPart = metaDocPart;
            this.level = parentDocPartMaterializer == null ? 0 : parentDocPartMaterializer.level + 1;
            this.keyDimension = keyDimension;
            this.rootDocPartData = parentDocPartMaterializer == null ? new DocPartData(this) : parentDocPartMaterializer.getRootDocPartData();
            this.docPartData = parentDocPartMaterializer == null ? this.rootDocPartData : new DocPartData(this);
        }
        
        public MutableMetaCollection getMetaCollection() {
            return metaCollection;
        }
        
        public MutableMetaDocPart getMetaDocPart() {
            return metaDocPart;
        }
        
        public DocPartMaterializer getKeyDimensionParentDocPartMaterializer() {
            return keyDimensionParentDocPartMaterializer;
        }
        
        public DocPartMaterializer getParentDocPartMaterializer() {
            return parentDocPartMaterializer;
        }
        
        public KeyDimension getKeyDimension() {
            return keyDimension;
        }
        
        public boolean isRoot() {
            return parentDocPartMaterializer == null;
        }
        
        public DocPartData getDocPartData() {
            return docPartData;
        }
        
        public DocPartData getRootDocPartData() {
            return rootDocPartData;
        }
        
        public Iterator<DocPartMaterializer> getChildDocPartMaterializerIterator() {
            return new Iterator<DocumentMaterializerVisitor.DocPartMaterializer>() {
                private final Iterator<Map.Entry<KeyDimension, DocPartMaterializer>> iterator = childMap.entrySet().iterator();
                
                @Override
                public boolean hasNext() {
                    return iterator.hasNext();
                }

                @Override
                public DocPartMaterializer next() {
                    return iterator.next().getValue();
                }
            };
        }
        
        public DocPartMaterializer beginAppendRootObject() {
            docPartData.appendRootRow();
            return this;
        }
        
        public DocPartMaterializer beginAppendObject(KeyDimension keyDimension) {
            DocPartMaterializer docPartMaterializer = append(null, keyDimension);
            docPartMaterializer.docPartData.appendObjectRow();
            return docPartMaterializer;
        }
        
        public void endAppendObject() {
            if (!isRoot()) {
                getParentDocPartMaterializer().appendColumnValue(keyDimension.getIdentifier(), FieldType.CHILD, KVBoolean.FALSE);
            }
        }
        
        public DocPartMaterializer beginAppendArray(KeyDimension keyDimension) {
            DocPartMaterializer docPartMaterializer = append(getKeyDimensionParentDocPartMaterializer(), keyDimension);
            return docPartMaterializer;
        }
        
        public void endAppendArray() {
            if (!isRoot()) {
                getParentDocPartMaterializer().appendColumnValue(keyDimension.getIdentifier(), FieldType.CHILD, KVBoolean.TRUE);
            }
        }
        
        private DocPartMaterializer append(DocPartMaterializer keyDimensionParentDocPartMaterializer, KeyDimension keyDimension) {
            DocPartMaterializer childDocPartMaterializer = this.keyDimensionParentDocPartMaterializer.childMap.get(keyDimension);
            
            if (childDocPartMaterializer == null) {
                TableRef tableRef = TableRefImpl.createChild(this.tableRef, keyDimension.getIdentifier());
                childDocPartMaterializer = new DocPartMaterializer(metaCollection, tableRef, this, keyDimensionParentDocPartMaterializer, keyDimension);
                this.keyDimensionParentDocPartMaterializer.childMap.put(keyDimension, childDocPartMaterializer);
            }
            
            return childDocPartMaterializer;
        }
        
        public void appendElement(int seq) {
            docPartData.appendArrayRow(seq);
        }
        
        public void appendKeyValue(String key, KVValue<?> value) {
            FieldType fieldType = FieldType.from(value.getType());
            appendColumnValue(key, fieldType, value);
        }
        
        public void appendScalarValue(KVValue<?> value) {
            appendKeyValue("v", value);
        }
        
        private void appendColumnValue(String key, FieldType fieldType, KVValue<?> value) {
            MetaField metaField = metaDocPart.getMetaFieldByNameAndType(key, fieldType);
            if (metaField == null) {
                String identifier = generateColumnName(key, fieldType);
                metaField = metaDocPart.addMetaField(key, identifier, fieldType);
            }
            docPartData.appendColumnValue(metaField.getName(), metaField.getIdentifier(), fieldType, value);
        }
    }
    
    public class KeyFieldType {
        private final String key;
        private final FieldType fieldType;
        
        public KeyFieldType(@Nonnull String key, @Nonnull FieldType fieldType) {
            super();
            this.key = key;
            this.fieldType = fieldType;
        }
        
        public String getKey() {
            return key;
        }

        public FieldType getFieldType() {
            return fieldType;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + fieldType.ordinal();
            result = prime * result + key.hashCode();
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            KeyFieldType other = (KeyFieldType) obj;
            if (fieldType != other.fieldType)
                return false;
            if (key == null) {
                if (other.key != null)
                    return false;
            } else if (!key.equals(other.key))
                return false;
            return true;
        }

        @Override
        public String toString() {
            return key + ':' + fieldType.name();
        }
    }
    
    public class DocPartData extends ArrayList<DocPartRow> {
        private static final long serialVersionUID = 1L;
        
        private final DocPartMaterializer docPartMaterializer;
        private final DocPartRidGenerator docPartRidGenerator;
        private final DocPartData parentDocPartData;
        private final DocPartData rootDocPartData;
        private final BiMap<KeyFieldType, Integer> columnIndexMap = HashBiMap.create();
        
        private int currentIndex = 0;
        private DocPartRow currentRow = null;
        
        public DocPartData(DocPartMaterializer docPartMaterializer) {
            super();
            this.docPartMaterializer = docPartMaterializer;
            this.docPartRidGenerator = new DocPartRidGenerator(docPartMaterializer);
            this.parentDocPartData = docPartMaterializer.isRoot() ? null : docPartMaterializer.getParentDocPartMaterializer().getDocPartData();
            this.rootDocPartData = docPartMaterializer.getRootDocPartData();
        }

        public DocPartRow currentRow() {
            return currentRow;
        }
        
        public int columnCount() {
            return columnIndexMap.size();
        }
        
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
                    return docPartMaterializer.getMetaDocPart().getMetaFieldByNameAndType(keyFieldType.getKey(), keyFieldType.getFieldType());
                }
            };
        }
        
        public void appendRootRow() {
            appendRow(new DocPartRow(this, 
                    docPartRidGenerator.nextRid()));
        }
        
        public void appendObjectRow() {
            appendRow(new DocPartRow(this, 
                    docPartRidGenerator.nextRid()));
        }
        
        public void appendArrayRow(int seq) {
            appendRow(new DocPartRow(this, 
                    rootDocPartData.currentRow().getDid(), 
                    parentDocPartData.currentRow().getRid(), 
                    docPartRidGenerator.nextRid(), seq));
        }

        private void appendRow(DocPartRow row) {
            currentRow = row;
            add(row);
        }
        
        public DocPartRow getCurrentRow() {
            return currentRow;
        }
        
        public void appendColumnValue(String key, String identifier, FieldType fieldType, KVValue<?> value) {
            KeyFieldType keyFieldType = new KeyFieldType(key, fieldType);
            Integer index = columnIndexMap.get(keyFieldType);
            if (index == null) {
                index = currentIndex++;
                columnIndexMap.put(keyFieldType, index);
            }
            currentRow.appendColumnValue(identifier, value, index);
        }
    }
    
    public class DocPartRow extends ArrayList<String> {
        private static final long serialVersionUID = 1L;
        
        private final DocPartData docPartData;
        private final int did;
        private final int rid;
        private final Integer pid;
        private final Integer seq;
        
        public DocPartRow(DocPartData docPartData, int did) {
            this.docPartData = docPartData;
            this.did = this.rid = did;
            this.pid = null;
            this.seq = null;
        }
        
        public DocPartRow(DocPartData docPartData, int did, int rid, int pid) {
            this.docPartData = docPartData;
            this.did = did;
            this.rid = rid;
            this.pid = pid;
            this.seq = null;
        }
        
        public DocPartRow(DocPartData docPartData, int did, int rid, int pid, int index) {
            this.docPartData = docPartData;
            this.did = did;
            this.rid = rid;
            this.pid = pid;
            this.seq = index;
        }
        
        public int getDid() {
            return did;
        }

        public int getRid() {
            return rid;
        }

        public Integer getPid() {
            return pid;
        }

        public Integer getSeq() {
            return seq;
        }
        
        @Override
        public Iterator<String> iterator() {
            return new Iterator<String>() {
                private final int globalCount = docPartData.columnCount();
                private final int count = size();
                private int index = 0;
                
                @Override
                public boolean hasNext() {
                    return index < globalCount;
                }

                @Override
                public String next() {
                    if (index < count) {
                        return get(index++);
                    }
                    
                    index++;
                    
                    return null;
                }
            };
        }
        
        public void appendColumnValue(String identifier, KVValue<?> value, int index) {
            final int size = size();
            if (index < size) {
                add(index, value.toString());
            } else {
                for (int offset = size; offset < index; offset++) {
                    add(null);
                }
                add(value.toString());
            }
        }
    }
    
    //TODO: Move and refactor
    private static String generateTableName(DocPartMaterializer docPart, KeyDimension newKeyDimension) {
        StringBuilder tableNameBuilder = new StringBuilder();
        List<String> translatedKeys = Lists.newArrayList();
        List<KeyDimension> keyDimensions = Lists.newArrayList();
        keyDimensions.add(newKeyDimension);
        while(docPart != null) {
            keyDimensions.add(docPart.getKeyDimension());
            docPart = docPart.getParentDocPartMaterializer();
        }
        Iterator<KeyDimension> keyDimensionIterator = keyDimensions.iterator();
        while (keyDimensionIterator.hasNext()) {
            KeyDimension keyDimension = keyDimensionIterator.next();
            
            String name = keyDimension.getIdentifier();
            translatedKeys.add(0, name.toLowerCase(Locale.US).replaceAll("[^a-z0-9$]", "_"));
            
            if (keyDimension.dimension > 1) {
                for (int dimension = keyDimension.dimension; dimension > 1; dimension--) {
                    keyDimensionIterator.next();
                }
            }
        }
        for (String translatedKey : translatedKeys) {
            tableNameBuilder.append(translatedKey);
            tableNameBuilder.append('_');
        }
        String tableName = tableNameBuilder.substring(0, tableNameBuilder.length() - 1);
        tableName = tableName.replaceAll("_+", "_");
        
        if (tableName.length() <= 63) {
            return tableName;
        }
        
        tableNameBuilder.delete(0, tableNameBuilder.length());
        tableNameBuilder.append(translatedKeys.get(0));
        for (int index = 1; index < translatedKeys.size() - 1; index++) {
            tableNameBuilder.append(translatedKeys.get(index).charAt(0));
            tableNameBuilder.append('_');
        }
        
        //TODO: Check internal and system table names and conflicts among existing metadata
        
        return tableName;
    }
    
    /*
    0 BINARY,
    1 BOOLEAN,
    2 DATE,
    3 DOUBLE,
    4 INSTANT,
    5 INTEGER,
    6 LONG,
    7 MONGO_OBJECT_ID,
    8 MONGO_TIME_STAMP,
    9 NULL,
    10 STRING,
    11 TIME,
    12 CHILD;
    */
    private static final char[] FIELD_TYPE_IDENTIFIERS = new char[] {'r', 'b', 't', 'd', 'k', 'i', 'l', 'x', 'y', 'n', 's', 'c', 'e'};

    //TODO: Move and refactor
    private static String generateColumnName(String key, FieldType fieldType) {
        char typeIdentifier = FIELD_TYPE_IDENTIFIERS[fieldType.ordinal()];
        String columnName = key.toLowerCase(Locale.US).replaceAll("[^a-z0-9$]", "_") + '_' + typeIdentifier;
        if (columnName.charAt(0) == '$' || (columnName.charAt(0) >= '0' && columnName.charAt(0) <= '9')) {
            columnName = '_' + columnName;
        }
        columnName = columnName.replaceAll("_+", "_");
        
        //TODO: Check internal and system column names and conflicts among existing metadata
        
        return columnName;
    }
    
    public static class DocPartRidGenerator {
        private final DocPartMaterializer docPartMaterializer;
        
        public DocPartRidGenerator(DocPartMaterializer docPartMaterializer) {
            this.docPartMaterializer = docPartMaterializer;
        }
        
        //TODO: Move and refactor
        public int nextRid() {
            Integer lastRid;
            
            synchronized (lastRidMap) {
                lastRid = lastRidMap.get(docPartMaterializer);
                
                if (lastRid == null) {
                    lastRid = 0;
                }
                
                lastRidMap.put(docPartMaterializer, lastRid + 1);
            }
            
            return lastRid;
        }
    }
    
    public static class ArrayAppendValueVisitor extends AppendValueVisitor {
        protected void appendValue(KVValue<?> value, Materializer arg) {
            arg.appendScalarValue(value);
        }
    }
    
    public static class ObjectAppendValueVisitor extends AppendValueVisitor {
        protected void appendValue(KVValue<?> value, Materializer arg) {
            arg.appendKeyValue(value);
        }
    }
    
    public static abstract class AppendValueVisitor implements KVValueVisitor<Void, DocumentMaterializerVisitor.Materializer> {
        protected abstract void appendValue(KVValue<?> value, Materializer arg);
        
        @Override
        public Void visit(KVInteger value, Materializer arg) {
            appendValue(value, arg);
            return null;
        }

        @Override
        public Void visit(KVLong value, Materializer arg) {
            appendValue(value, arg);
            return null;
        }

        @Override
        public Void visit(KVString value, Materializer arg) {
            appendValue(value, arg);
            return null;
        }
        @Override
        public Void visit(KVDouble value, Materializer arg) {
            appendValue(value, arg);
            return null;
        }

        @Override
        public Void visit(KVBoolean value, Materializer arg) {
            appendValue(value, arg);
            return null;
        }

        @Override
        public Void visit(KVNull value, Materializer arg) {
            appendValue(value, arg);
            return null;
        }

        @Override
        public Void visit(KVMongoObjectId value, Materializer arg) {
            appendValue(value, arg);
            return null;
        }

        @Override
        public Void visit(KVInstant value, Materializer arg) {
            appendValue(value, arg);
            return null;
        }

        @Override
        public Void visit(KVDate value, Materializer arg) {
            appendValue(value, arg);
            return null;
        }

        @Override
        public Void visit(KVTime value, Materializer arg) {
            appendValue(value, arg);
            return null;
        }
        
        @Override
        public Void visit(KVBinary value, Materializer arg) {
            appendValue(value, arg);
            return null;
        }

        @Override
        public Void visit(KVMongoTimestamp value, Materializer arg) {
            appendValue(value, arg);
            return null;
        }
        
        @Override
        public Void visit(KVDocument value, Materializer arg) {
            return null;
        }
        
        @Override
        public Void visit(KVArray value, Materializer arg) {
            return null;
        }
    }
}
