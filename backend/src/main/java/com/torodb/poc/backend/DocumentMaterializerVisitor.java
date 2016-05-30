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
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import javax.annotation.Nonnull;

import org.jooq.DataType;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
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

    public CollectionMaterializer createCollectionMaterialized(CollectionSnapshot collection) {
        return new CollectionMaterializer(collection);
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
        public String getKey();

        public DocPartMaterializer getDocPartMaterializer();
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
        private final CollectionSnapshot collection;
        private final Map<String, PartMaterializer> collectionRootMaterializerMap = Maps.newHashMap();
        
        public CollectionMaterializer(CollectionSnapshot collection) {
            super();
            this.collection = collection;
        }

        public String getKey() {
            return collection.getName();
        }

        public DocPartMaterializer getDocPartMaterializer() {
            return null;
        }
        
        public Materializer beginAppendObject() {
            PartMaterializer partMaterializer = collectionRootMaterializerMap.get(collection.getName());
            
            if (partMaterializer == null) {
                partMaterializer = new PartMaterializer(collection.getName(), new DocPartMaterializer(collection.getRootDocPartSnapshot(), null, null));
                collectionRootMaterializerMap.put(collection.getName(), partMaterializer);
            }
            
            return partMaterializer;
        }
        
        public void endAppendObject() {
            PartMaterializer partMaterializer = collectionRootMaterializerMap.get(collection.getName());
            partMaterializer.endAppendObject();
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
            return new PartMaterializer(key, docPartMaterializer.baginAppendObject(key));
        }
        
        public void endAppendObject() {
            docPartMaterializer.endAppendObject();
        }
        
        public Materializer beginAppendArray() {
            return new PartMaterializer(key, docPartMaterializer.baginAppendArray());
        }
        
        public Materializer appendArrayElement(int index) {
            docPartMaterializer.appendElement(index);
            return this;
        }
        
        public void endAppendArray() {
            docPartMaterializer.endAppendArray();
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
    
    public class DocPartMaterializer implements DocPart {
        private final DocPartSnapshot docPartSnapshot;
        private final DocPartMaterializer parentDocPartMaterializer;
        private final int level;
        private final KeyDimension keyDimension;
        private final String tableName;
        private final DocPartData docPartData;
        private final DocPartData rootDocPartData;
        private final Map<KeyDimension, DocPartMaterializer> childMap = Maps.newHashMap();
        
        private DocPartMaterializer(DocPartSnapshot docPartSnapshot, DocPartMaterializer parentDocPartMaterializer, KeyDimension keyDimension) {
            super();
            this.docPartSnapshot = docPartSnapshot;
            this.parentDocPartMaterializer = parentDocPartMaterializer;
            this.level = parentDocPartMaterializer == null ? 0 : parentDocPartMaterializer.getLevel() + 1;
            this.keyDimension = keyDimension;
            this.tableName = docPartSnapshot.getTableName();
            this.docPartData = new DocPartData(this);
            this.rootDocPartData = parentDocPartMaterializer == null ? this.docPartData : parentDocPartMaterializer.getDocPartData();
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
        
        public DocPartData getDocPartData() {
            return docPartData;
        }
        
        public DocPartData getRootDocPartData() {
            return rootDocPartData;
        }
        
        @Override
        public String getTableName() {
            return tableName;
        }
        
        @Override
        public String getParentTableName() {
            return isRoot() ? null : parentDocPartMaterializer.tableName;
        }
        
        @Override
        public int getLevel() {
            return level;
        }
        
        @Override
        public boolean isRoot() {
            return parentDocPartMaterializer == null;
        }
        
        public DocPartMaterializer baginAppendObject(String key) {
            KeyDimension keyDimension = new KeyDimension(key, 0);
            DocPartMaterializer docPartMaterializer = append(keyDimension);
            return docPartMaterializer;
        }
        
        public void endAppendObject() {
            if (childMap.isEmpty()) {
                appendKeyValueRaw("has_child", KVNull.getInstance());
            } else {
                appendKeyValueRaw("has_child", KVBoolean.TRUE);
            }
        }
        
        public DocPartMaterializer baginAppendArray() {
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
                child = new DocPartMaterializer(docPartSnapshot, this, keyDimension);
                childMap.put(keyDimension, child);
            }
            
            return child;
        }
        
        public void appendElement(int seq) {
            docPartData.appendArrayRow(seq);
        }
        
        public void appendKeyValue(String key, KVValue<?> value) {
            docPartData.appendColumnValue(key, value);
        }
        
        public void appendScalarValue(KVValue<?> value) {
            docPartData.appendColumnValue(value);
        }
        
        public void appendKeyValueRaw(String key, KVValue<?> value) {
            docPartData.appendColumnValueRaw(key, value);
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
    
    public class DocPartData extends ArrayList<TableRow> implements TableData {
        private static final long serialVersionUID = 1L;
        
        private final DocPartRidGenerator docPartRidGenerator;
        private final DocPartMaterializer docPartMaterializer;
        private final DocPartData parentDocPartData;
        private final DocPartData rootDocPartData;
        private final Map<KeyTypeId, Integer> columnIndexMap = Maps.newHashMap();
        
        private int currentIndex = 0;
        private DocumentDocPartRow currentRow = null;
        
        public DocPartData(DocPartMaterializer docPartMaterializer) {
            super();
            this.docPartRidGenerator = new DocPartRidGenerator(docPartMaterializer);
            this.docPartMaterializer = docPartMaterializer;
            this.parentDocPartData = docPartMaterializer.getDocPartData();
            this.rootDocPartData = docPartMaterializer.getRootDocPartData();
        }

        public DocPartMaterializer getDocPart() {
            return docPartMaterializer;
        }
        
        public DocumentDocPartRow currentRow() {
            return currentRow;
        }
        
        public int columnCount() {
            return columnIndexMap.size();
        }
        
        public void appendRootRow() {
            appendRow(new DocumentDocPartRow(this, 
                    docPartRidGenerator.nextRid()));
        }
        
        public void appendObjectRow() {
            appendRow(new DocumentDocPartRow(this, 
                    rootDocPartData.currentRow().getDid(), 
                    parentDocPartData.currentRow().getRid(), 
                    docPartRidGenerator.nextRid()));
        }
        
        public void appendArrayRow(int seq) {
            appendRow(new DocumentDocPartRow(this, 
                    rootDocPartData.currentRow().getDid(), 
                    parentDocPartData.currentRow().getRid(), 
                    docPartRidGenerator.nextRid(), seq));
        }

        private void appendRow(DocumentDocPartRow row) {
            currentRow = row;
            add(row);
        }
        
        public DocumentDocPartRow getCurrentRow() {
            return currentRow;
        }
        
        public void appendColumnValue(String key, KVValue<?> value) {
            appendColumnValueRaw(key, value);
        }
        
        public void appendColumnValueRaw(String key, KVValue<?> value) {
            KeyTypeId keyTypeId = new KeyTypeId(key, value.getType().accept(typeIdVisitor, null));
            Integer index = columnIndexMap.get(keyTypeId);
            if (index == null) {
                index = currentIndex++;
                columnIndexMap.put(keyTypeId, index);
            }
            currentRow.appendColumnValue(key, value, index);
        }
        
        public void appendColumnValue(KVValue<?> value) {
            appendColumnValue("v", value);
        }
    }
    
    public class DocumentDocPartRow extends ArrayList<String> implements TableRow {
        private static final long serialVersionUID = 1L;
        
        private final DocPartData docPartData;
        private final int did;
        private final int rid;
        private final Integer pid;
        private final Integer seq;
        
        public DocumentDocPartRow(DocPartData docPartData, int did) {
            this.docPartData = docPartData;
            this.did = this.rid = did;
            this.pid = null;
            this.seq = null;
        }
        
        public DocumentDocPartRow(DocPartData docPartData, int did, int rid, int pid) {
            this.docPartData = docPartData;
            this.did = did;
            this.rid = rid;
            this.pid = pid;
            this.seq = null;
        }
        
        public DocumentDocPartRow(DocPartData docPartData, int did, int rid, int pid, int index) {
            this.docPartData = docPartData;
            this.did = did;
            this.rid = rid;
            this.pid = pid;
            this.seq = index;
        }
        
        @Override
        public int getDid() {
            return did;
        }

        @Override
        public int getRid() {
            return rid;
        }

        @Override
        public Integer getPid() {
            return pid;
        }

        @Override
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
        
        public void appendColumnValue(String key, KVValue<?> value, int index) {
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
    
    public interface DatabaseSnapshot {
        public String getSchemaName();
        public CollectionSnapshot getCollectionSnapshot(String collectionName);
    }
    
    public interface CollectionSnapshot {
        public String getName();
        public DatabaseSnapshot getDatabaseSnapshot();
        public DocPartSnapshot getRootDocPartSnapshot();
        public boolean hasDocPartSnapshot(DocPart docPart);
        public DocPartSnapshot getDocPartSnapshot(DocPart docPart);
        public DocPartSnapshot appendDocPartSnapshot(DocPart docPart);
    }
    
    public interface DocPartSnapshot {
        public String getDocPart();
        public CollectionSnapshot getCollectionSnapshot();
        public String getTableName();
        public boolean hasFieldSnapshot(String name, KVType type);
        public FieldSnapshot getFieldSnapshot(String name, KVType type);
        public FieldSnapshot appendFieldSnapshot(String key, KVType type);
    }
    
    public interface FieldSnapshot {
        public String getColumnName();
        public DataType<?> getColumnType();
        public DocPartSnapshot getDocPartSnapshot();
    }
    
    public interface DocPart {
        public String getTableName();
        public String getParentTableName();
        public int getLevel();
        public boolean isRoot();
    }
    
    public interface TableBulk extends Iterable<TableData> {
    }
    
    public interface TableData extends Collection<TableRow> {
        public DocPart getDocPart();
    }
    
    public interface TableRow extends Iterable<String> {
        public int getDid();
        public int getRid();
        public Integer getPid();
        public Integer getSeq();
    }
    
    //TODO: Move and refactor
    private static String generateTableName(DocPartMaterializer docPart) {
        StringBuilder tableNameBuilder = new StringBuilder();
        List<String> translatedKeys = Lists.newArrayList();
        while(docPart != null) {
            String name = docPart.isRoot() ? docPart.docPartSnapshot.getCollectionSnapshot().getName() : docPart.keyDimension.getIdentifier();
            translatedKeys.add(0, name.toLowerCase(Locale.US).replaceAll("[^a-z0-9$]", "_"));
            docPart = docPart.parentDocPartMaterializer;
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
