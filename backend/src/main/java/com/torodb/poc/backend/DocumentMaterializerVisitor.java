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
import java.util.Optional;

import javax.annotation.Nonnull;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.torodb.core.TableRef;
import com.torodb.core.transaction.metainf.MetaField;
import com.torodb.core.transaction.metainf.MutableMetaCollection;
import com.torodb.core.transaction.metainf.MutableMetaDocPart;
import com.torodb.kvdocument.types.ArrayType;
import com.torodb.kvdocument.types.BinaryType;
import com.torodb.kvdocument.types.BooleanType;
import com.torodb.kvdocument.types.DateType;
import com.torodb.kvdocument.types.DocumentType;
import com.torodb.kvdocument.types.DoubleType;
import com.torodb.kvdocument.types.GenericType;
import com.torodb.kvdocument.types.InstantType;
import com.torodb.kvdocument.types.IntegerType;
import com.torodb.kvdocument.types.KVType;
import com.torodb.kvdocument.types.KVTypeVisitor;
import com.torodb.kvdocument.types.LongType;
import com.torodb.kvdocument.types.MongoObjectIdType;
import com.torodb.kvdocument.types.MongoTimestampType;
import com.torodb.kvdocument.types.NonExistentType;
import com.torodb.kvdocument.types.NullType;
import com.torodb.kvdocument.types.StringType;
import com.torodb.kvdocument.types.TimeType;
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
    private static final TypeIdVisitor typeIdVisitor = new TypeIdVisitor();
    private static final TypeIdentifierVisitor typeIdentifierVisitor = new TypeIdentifierVisitor();
    
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

    public CollectionMaterializer getCollectionMaterializer(MutableMetaCollection<MutableMetaDocPart> metaCollection) {
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
        
        public KeyDimension(@Nonnull String key, int dimension) {
            super();
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
        
        public CollectionMaterializer(MutableMetaCollection<MutableMetaDocPart> metaCollection) {
            super();
            TableRef rootTableRef = TableRefImpl.createRoot();
            this.rootPartMaterializer = new PartMaterializer(metaCollection.getName(), new DocPartMaterializer(metaCollection, rootTableRef, null, null));
        }
        
        public PartMaterializer getRootPartMaterializer() {
            return rootPartMaterializer;
        }

        public Materializer beginAppendObject() {
            rootPartMaterializer.beginAppendObject();
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
        private final String key;
        private final DocPartMaterializer docPartMaterializer;
        
        public PartMaterializer(String key, DocPartMaterializer docPartMaterializer) {
            super();
            this.key = key;
            this.docPartMaterializer = docPartMaterializer;
        }

        public String getKey() {
            return key;
        }

        public DocPartMaterializer getDocPartMaterializer() {
            return docPartMaterializer;
        }
        
        public Materializer beginAppendObject() {
            return new PartMaterializer(key, docPartMaterializer.beginAppendObject(key));
        }
        
        public void endAppendObject() {
            docPartMaterializer.endAppendObject();
        }
        
        public Materializer beginAppendArray() {
            return new PartMaterializer(key, docPartMaterializer.beginAppendArray());
        }
        
        public void endAppendArray() {
            docPartMaterializer.endAppendArray();
        }
        
        public Materializer appendArrayElement(int index) {
            docPartMaterializer.appendElement(index);
            return this;
        }
        
        public Materializer appendValueFromObject(String key, KVValue<?> value) {
            value.accept(objectAppendValueVisitor, this);
            return new PartMaterializer(key, docPartMaterializer);
        }
        
        public Materializer appendValueFromArray(KVValue<?> value) {
            value.accept(arrayAppendValueVisitor, this);
            return this;
        }
        
        public void appendKeyValue(KVValue<?> value) {
            docPartMaterializer.appendKeyValue(key, value);
        }
        
        public void appendScalarValue(KVValue<?> value) {
            docPartMaterializer.appendScalarValue(value);
        }
    }
    
    public class DocPartMaterializer {
        private final MutableMetaCollection<MutableMetaDocPart> metaCollection;
        private final MutableMetaDocPart metaDocPart;
        private final TableRef tableRef;
        private final DocPartMaterializer parentDocPartMaterializer;
        private final int level;
        private final KeyDimension keyDimension;
        private final DocPartData docPartData;
        private final DocPartData rootDocPartData;
        private final Map<KeyDimension, DocPartMaterializer> childMap = Maps.newHashMap();
        
        private DocPartMaterializer(MutableMetaCollection<MutableMetaDocPart> metaCollection, TableRef tableRef, DocPartMaterializer parentDocPartMaterializer, KeyDimension keyDimension) {
            super();
            this.metaCollection = metaCollection;
            MutableMetaDocPart metaDocPart = metaCollection.getMetaDocPartByTableRef(tableRef);
            if (metaDocPart == null) {
                String identifier = generateTableName(parentDocPartMaterializer, keyDimension);
                metaDocPart = metaCollection.addMetaDocPart(tableRef, identifier);
            }
            this.metaDocPart = metaDocPart;
            this.tableRef = tableRef;
            this.parentDocPartMaterializer = parentDocPartMaterializer;
            this.level = parentDocPartMaterializer == null ? 0 : parentDocPartMaterializer.level + 1;
            this.keyDimension = keyDimension;
            this.docPartData = new DocPartData(this);
            this.rootDocPartData = parentDocPartMaterializer == null ? this.docPartData : parentDocPartMaterializer.getDocPartData();
        }
        
        public MutableMetaCollection<MutableMetaDocPart> getMetaCollection() {
            return metaCollection;
        }
        
        public MutableMetaDocPart getMetaDocPart() {
            return metaDocPart;
        }
        
        public DocPartMaterializer getParentDocPartMaterializer() {
            return parentDocPartMaterializer;
        }

        public String getKey() {
            return keyDimension.key;
        }

        public int getDimension() {
            return keyDimension.dimension;
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
        
        public Iterator<DocPartMaterializer> childDocPartMaterializerIterator() {
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
        
        public DocPartMaterializer beginAppendObject(String key) {
            KeyDimension keyDimension = new KeyDimension(key, 0);
            DocPartMaterializer docPartMaterializer = append(keyDimension);
            return docPartMaterializer;
        }
        
        public void endAppendObject() {
            if (!childMap.isEmpty()) {
                appendKeyValue("has_child", KVBoolean.TRUE);
            }
        }
        
        public DocPartMaterializer beginAppendArray() {
            KeyDimension keyDimension = new KeyDimension(getKey(), getDimension() + 1);
            DocPartMaterializer docPartMaterializer = append(keyDimension);
            return docPartMaterializer;
        }
        
        public void endAppendArray() {
            endAppendObject();
        }
        
        private DocPartMaterializer append(KeyDimension keyDimension) {
            DocPartMaterializer child = childMap.get(keyDimension);
            
            if (child == null) {
                TableRef tableRef = TableRefImpl.createChild(this.tableRef, keyDimension.getIdentifier());
                child = new DocPartMaterializer(metaCollection, tableRef, this, keyDimension);
                childMap.put(keyDimension, child);
            }
            
            return child;
        }
        
        public void appendElement(int seq) {
            docPartData.appendArrayRow(seq);
        }
        
        public void appendKeyValue(String key, KVValue<?> value) {
            MetaField metaField = metaDocPart.getMetaFieldByNameAndType(key, value.getType());
            if (metaField == null) {
                String identifier = generateColumnName(key, value.getType());
                metaField = metaDocPart.addMetaField(key, identifier, value.getType());
            }
            appendColumnValue(metaField, value);
        }
        
        public void appendScalarValue(KVValue<?> value) {
            appendKeyValue("v", value);
        }
        
        private void appendColumnValue(MetaField metaField, KVValue<?> value) {
            docPartData.appendColumnValue(metaField.getName(), metaField.getIdentifier(), value);
        }
    }
    
    public static class TableRefImpl extends TableRef {
        private final Optional<TableRef> parent;
        private final String name;
        
        public static TableRef createRoot() {
            return new TableRefImpl();
        }
        
        public static TableRef createChild(TableRef tableRef, String name) {
            return new TableRefImpl(tableRef, name);
        }
        
        private TableRefImpl() {
            super();
            this.parent = Optional.empty();
            this.name = "";
        }
        
        private TableRefImpl(@Nonnull TableRef parent, @Nonnull String name) {
            super();
            this.parent = Optional.of(parent);
            this.name = name;
        }
        
        @Override
        public Optional<TableRef> getParent() {
            return parent;
        }

        @Override
        public String getName() {
            return name;
        }
    }
    
    public class KeyTypeId {
        private final String key;
        private final int typeId;
        
        public KeyTypeId(@Nonnull String key, int typeId) {
            super();
            this.key = key;
            this.typeId = typeId;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + key.hashCode();
            result = prime * result + typeId;
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            KeyTypeId other = (KeyTypeId) obj;
            if (key == null) {
                if (other.key != null)
                    return false;
            } else if (!key.equals(other.key))
                return false;
            if (typeId != other.typeId)
                return false;
            return true;
        }
    }
    
    public class DocPartData extends ArrayList<DocPartRow> {
        private static final long serialVersionUID = 1L;
        
        private final DocPartRidGenerator docPartRidGenerator;
        private final DocPartData parentDocPartData;
        private final DocPartData rootDocPartData;
        private final BiMap<KeyTypeId, Integer> columnIndexMap = HashBiMap.create();
        
        private int currentIndex = 0;
        private DocPartRow currentRow = null;
        
        public DocPartData(DocPartMaterializer docPartMaterializer) {
            super();
            this.docPartRidGenerator = new DocPartRidGenerator(docPartMaterializer);
            this.parentDocPartData = docPartMaterializer.getDocPartData();
            this.rootDocPartData = docPartMaterializer.getRootDocPartData();
        }

        public DocPartRow currentRow() {
            return currentRow;
        }
        
        public int columnCount() {
            return columnIndexMap.size();
        }
        
        public Iterator<KeyTypeId> orderedKeyTypeIdIterator() {
            return new Iterator<DocumentMaterializerVisitor.KeyTypeId>() {
                private final int count = columnCount();
                private int index = 0;
                
                @Override
                public boolean hasNext() {
                    return index < count;
                }
                
                @Override
                public KeyTypeId next() {
                    return columnIndexMap.inverse().get(index++);
                }
            };
        }
        
        public void appendRootRow() {
            appendRow(new DocPartRow(this, 
                    docPartRidGenerator.nextRid()));
        }
        
        public void appendObjectRow() {
            appendRow(new DocPartRow(this, 
                    rootDocPartData.currentRow().getDid(), 
                    parentDocPartData.currentRow().getRid(), 
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
        
        public void appendColumnValue(String key, String identifier, KVValue<?> value) {
            KeyTypeId keyTypeId = new KeyTypeId(key, value.getType().accept(typeIdVisitor, null));
            Integer index = columnIndexMap.get(keyTypeId);
            if (index == null) {
                index = currentIndex++;
                columnIndexMap.put(keyTypeId, index);
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
                private final int count = docPartData.columnCount();
                private int index = 0;
                
                @Override
                public boolean hasNext() {
                    return index < globalCount;
                }

                @Override
                public String next() {
                    return index < count ? get(index) : null;
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
    private static String generateTableName(DocPartMaterializer docPart, KeyDimension keyDimension) {
        StringBuilder tableNameBuilder = new StringBuilder();
        List<String> translatedKeys = Lists.newArrayList();
        while(translatedKeys.isEmpty() || docPart != null) {
            String name = translatedKeys.isEmpty() ? keyDimension.getIdentifier() : 
                docPart.isRoot() ? docPart.getMetaCollection().getName() : docPart.keyDimension.getIdentifier();
            docPart = translatedKeys.isEmpty() ? docPart : docPart.parentDocPartMaterializer;
            translatedKeys.add(0, name.toLowerCase(Locale.US).replaceAll("[^a-z0-9$]", "_"));
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
        
        return tableName;
    }
    
    //TODO: Move and refactor
    private static String generateColumnName(String key, KVType type) {
        Character typeIdentifier = type.accept(typeIdentifierVisitor, null);
        String columnName = key.toLowerCase(Locale.US).replaceAll("[^a-z0-9$]", "_") + typeIdentifier;
        if (columnName.charAt(0) == '$' || (columnName.charAt(0) >= '0' && columnName.charAt(0) <= '9')) {
            columnName = '_' + columnName;
        }
        columnName = columnName.replaceAll("_+", "_");
        
        //TODO: Check system column names
        
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
    
    public static class TypeIdentifierVisitor implements KVTypeVisitor<Character, Void> {
        @Override
        public Character visit(ArrayType type, Void arg) {
            throw new ToroImplementationException("Cannot identify type " + type.getClass());
        }

        @Override
        public Character visit(BooleanType type, Void arg) {
            return 'b';
        }

        @Override
        public Character visit(DoubleType type, Void arg) {
            return 'd';
        }

        @Override
        public Character visit(IntegerType type, Void arg) {
            return 'i';
        }

        @Override
        public Character visit(LongType type, Void arg) {
            return 'l';
        }

        @Override
        public Character visit(NullType type, Void arg) {
            return 'n';
        }

        @Override
        public Character visit(DocumentType type, Void arg) {
            throw new ToroImplementationException("Cannot identify type " + type.getClass());
        }

        @Override
        public Character visit(StringType type, Void arg) {
            return 's';
        }

        @Override
        public Character visit(GenericType type, Void arg) {
            throw new ToroImplementationException("Cannot identify type " + type.getClass());
        }

        @Override
        public Character visit(MongoObjectIdType type, Void arg) {
            return 'x';
        }

        @Override
        public Character visit(InstantType type, Void arg) {
            return 'k';
        }

        @Override
        public Character visit(DateType type, Void arg) {
            return 't';
        }

        @Override
        public Character visit(TimeType type, Void arg) {
            return 'c';
        }

        @Override
        public Character visit(BinaryType type, Void arg) {
            return 'r';
        }

        @Override
        public Character visit(NonExistentType type, Void arg) {
            throw new ToroImplementationException("Cannot identify type " + type.getClass());
        }

        @Override
        public Character visit(MongoTimestampType type, Void arg) {
            return 'y';
        }
    }
    
    public static class TypeIdVisitor implements KVTypeVisitor<Integer, Void> {
        @Override
        public Integer visit(ArrayType type, Void arg) {
            return 0;
        }

        @Override
        public Integer visit(BooleanType type, Void arg) {
            return 1;
        }

        @Override
        public Integer visit(DoubleType type, Void arg) {
            return 2;
        }

        @Override
        public Integer visit(IntegerType type, Void arg) {
            return 3;
        }

        @Override
        public Integer visit(LongType type, Void arg) {
            return 4;
        }

        @Override
        public Integer visit(NullType type, Void arg) {
            return 5;
        }

        @Override
        public Integer visit(DocumentType type, Void arg) {
            return 6;
        }

        @Override
        public Integer visit(StringType type, Void arg) {
            return 7;
        }

        @Override
        public Integer visit(GenericType type, Void arg) {
            return 8;
        }

        @Override
        public Integer visit(MongoObjectIdType type, Void arg) {
            return 9;
        }

        @Override
        public Integer visit(InstantType type, Void arg) {
            return 10;
        }

        @Override
        public Integer visit(DateType type, Void arg) {
            return 11;
        }

        @Override
        public Integer visit(TimeType type, Void arg) {
            return 12;
        }

        @Override
        public Integer visit(BinaryType type, Void arg) {
            return 13;
        }

        @Override
        public Integer visit(NonExistentType type, Void arg) {
            return 14;
        }

        @Override
        public Integer visit(MongoTimestampType type, Void arg) {
            return 15;
        }
    }
}
