package com.torodb.backend.d2r;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.jooq.Converter;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Table;
import com.torodb.backend.DatabaseInterface;
import com.torodb.backend.InternalField;
import com.torodb.backend.converters.jooq.DataTypeForKV;
import com.torodb.backend.interfaces.ErrorHandlerInterface.Context;
import com.torodb.backend.mocks.ToroImplementationException;
import com.torodb.backend.tables.MetaDocPartTable;
import com.torodb.core.TableRef;
import com.torodb.core.d2r.DocPartResult;
import com.torodb.core.d2r.DocPartResults;
import com.torodb.core.d2r.R2DTranslator;
import com.torodb.core.transaction.BackendException;
import com.torodb.core.transaction.RollbackException;
import com.torodb.core.transaction.metainf.FieldType;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.core.transaction.metainf.MetaField;
import com.torodb.core.transaction.metainf.MetaSnapshot;
import com.torodb.kvdocument.values.KVBoolean;
import com.torodb.kvdocument.values.KVDocument;
import com.torodb.kvdocument.values.KVValue;
import com.torodb.kvdocument.values.heap.ListKVArray;

public class R2DTranslatorImpl implements R2DTranslator<ResultSet> {

    private final DatabaseInterface databaseInterface;
    private final MetaDatabase metaDatabase;
    private final MetaCollection metaCollection;
	
	public R2DTranslatorImpl(DatabaseInterface databaseInterface, MetaSnapshot metaSnapshot, String dbName, String collectionName) {
	    this.databaseInterface = databaseInterface;
        MetaDatabase metaDatabase = metaSnapshot.getMetaDatabaseByName(dbName);
        MetaCollection metaCollection = metaDatabase.getMetaCollectionByName(collectionName);
		this.metaDatabase = metaDatabase;
		this.metaCollection = metaCollection;
	}

    @Override
    public Collection<KVDocument> translate(DocPartResults<ResultSet> docPartResultSets) throws BackendException, RollbackException {
        try {
            return tryTranslate(docPartResultSets);
        } catch(SQLException ex) {
            databaseInterface.handleRetryException(Context.fetch, ex);
            
            throw new BackendException(ex);
        }
    }
    
    private Collection<KVDocument> tryTranslate(DocPartResults<ResultSet> docPartResultSets) throws SQLException {
        //TODO: Remove this cast if we are always using jooq data type Integer for did, rid, pid and seq
        Converter<Object, Integer> didConverter = (Converter<Object, Integer>) 
                databaseInterface.getMetaDocPartTable().DID.getConverter();
        Converter<Object, Integer> ridConverter = (Converter<Object, Integer>) 
                databaseInterface.getMetaDocPartTable().RID.getConverter();
        Converter<Object, Integer> pidConverter = (Converter<Object, Integer>) 
                databaseInterface.getMetaDocPartTable().PID.getConverter();
        Converter<Object, Integer> seqConverter = (Converter<Object, Integer>) 
                databaseInterface.getMetaDocPartTable().SEQ.getConverter();
        
        ImmutableList.Builder<KVDocument> readedDocuments = ImmutableList.builder();
        Table<TableRef, String, Map<Integer, List<KVValue<?>>>> currentDocPartTable = 
                HashBasedTable.<TableRef, String, Map<Integer, List<KVValue<?>>>>create();
        Table<TableRef, String, Map<Integer, List<KVValue<?>>>> childDocPartTable = 
                HashBasedTable.<TableRef, String, Map<Integer, List<KVValue<?>>>>create();
        int previousLevel = -1;
        Iterator<DocPartResult<ResultSet>> docPartResultSetIterator = docPartResultSets.iterator();
        while(docPartResultSetIterator.hasNext()) {
            DocPartResult<ResultSet> docPartResultSet = docPartResultSetIterator.next();
            MetaDocPart metaDocPart = docPartResultSet.getMetaDocPart();
            TableRef tableRef = metaDocPart.getTableRef();
            
            if (previousLevel == -1 || previousLevel != tableRef.getDepth()) {
                Table<TableRef, String, Map<Integer, List<KVValue<?>>>> 
                    previousChildDocPartTable = childDocPartTable;
                childDocPartTable = currentDocPartTable;
                currentDocPartTable = previousChildDocPartTable;
                currentDocPartTable.clear();
            }
            previousLevel = tableRef.getDepth();
            
            Map<String, Map<Integer, List<KVValue<?>>>> childDocPartRow = 
                    childDocPartTable.row(tableRef);
            
            ResultSet resultSet = docPartResultSet.getResult();
            while (resultSet.next()) {
                Integer did = null;
                Integer pid = null;
                Integer rid = null;
                Integer seq = null;
                Collection<InternalField<?>> internalFields = databaseInterface.getDocPartTableInternalFields(
                        metaDocPart);
                int columnIndex = 1;
                for (InternalField<?> internalField : internalFields) {
                    if (internalField.isDid()) {
                        did = didConverter.from(resultSet.getObject(columnIndex));
                    } else if (internalField.isRid()) {
                        rid = ridConverter.from(resultSet.getObject(columnIndex));
                    } else if (internalField.isPid()) {
                        pid = pidConverter.from(resultSet.getObject(columnIndex));
                    } else if (internalField.isSeq()) {
                        seq = seqConverter.from(resultSet.getObject(columnIndex));
                    }
                    columnIndex++;
                }
                if (did == null) {
                    throw new ToroImplementationException("did was not found for doc part " + tableRef 
                            + " in collection " + metaCollection.getName() + " and database " + metaDatabase.getName());
                }
                
                if (rid == null) {
                    rid = did;
                }
                
                if (pid == null) {
                    pid = did;
                }
                
                KVDocument.Builder documentBuilder = new KVDocument.Builder();
                //TODO: ensure MetaField order using ResultSet meta data
                Iterator<? extends MetaField> metaFieldIterator = metaDocPart
                        .streamFields().iterator();
                boolean wasScalar = false;
                while (metaFieldIterator.hasNext()) {
                    MetaField metaField = metaFieldIterator.next();
                    Object databaseValue = resultSet.getObject(columnIndex);
                    columnIndex++;
                    
                    if (databaseValue == null) {
                        continue;
                    }
                    
                    DataTypeForKV<?> dataType = databaseInterface.getDataType(metaField.getType());
                    Converter<Object, KVValue<?>> converter = (Converter<Object, KVValue<?>>) dataType.getConverter();
                    KVValue<?> value = converter.from(databaseValue);
                    
                    if (metaField.getType() == FieldType.CHILD) {
                        KVBoolean child = (KVBoolean) value;
                        Map<Integer, List<KVValue<?>>> childDocPartCell = childDocPartRow.get(metaField.getName());
                        if (child.getValue()) {
                            List<KVValue<?>> elements;
                            if (childDocPartCell == null || (elements = childDocPartCell.get(rid)) == null) {
                                value = new ListKVArray(ImmutableList.of());
                            } else {
                                value = new ListKVArray(elements);
                            }
                        } else {
                            value = childDocPartCell.get(rid).get(0);
                        }
                    }
                    
                    if(metaField.getIdentifier().indexOf(MetaDocPartTable.DocPartTableFields.SCALAR.fieldName) == 0
                            && metaField.getIdentifier().length() == MetaDocPartTable.DocPartTableFields.SCALAR.fieldName.length() + 2) {
                        assert !tableRef.isRoot() : "found scalar value in root doc part";
                        Map<Integer, List<KVValue<?>>> currentDocPartCell = getDocPartCell(
                                currentDocPartTable, tableRef, seq);
                        if (seq == null) {
                            currentDocPartCell.put(pid, ImmutableList.of(value));
                        } else {
                            List<KVValue<?>> elements = getCellElements(currentDocPartCell, pid);
                            setElementValue(elements, seq, value);
                        }
                        wasScalar = true;
                        break;
                    } else {
                        documentBuilder.putValue(metaField.getName(), value);
                    }
                }
                
                if (wasScalar) {
                    continue;
                }
                
                if (tableRef.isRoot()) {
                    readedDocuments.add(documentBuilder.build());
                } else {
                    Map<Integer, List<KVValue<?>>> currentDocPartCell = getDocPartCell(
                            currentDocPartTable, tableRef, seq);
                    if (seq == null) {
                        currentDocPartCell.put(pid, ImmutableList.of(documentBuilder.build()));
                    } else {
                        List<KVValue<?>> elements = getCellElements(currentDocPartCell, pid);
                        setElementValue(elements, seq, documentBuilder.build());
                    }
                }
            }
        }
        
        
        return readedDocuments.build();
    }

    private Map<Integer, List<KVValue<?>>> getDocPartCell(
            Table<TableRef, String, Map<Integer, List<KVValue<?>>>> docPartTable, TableRef tableRef, Integer seq) {
        String name = tableRef.getName();
        
        if (seq != null && tableRef.isInArray()) {
            name = MetaDocPartTable.DocPartTableFields.SCALAR.fieldName;
        }
        
        return getDocPartCell(docPartTable, tableRef, name);
    }

    private Map<Integer, List<KVValue<?>>> getDocPartCell(
            Table<TableRef, String, Map<Integer, List<KVValue<?>>>> docPartTable, TableRef tableRef, String name) {
        Map<Integer, List<KVValue<?>>> docPartCell = 
                docPartTable.get(tableRef.getParent().get(), name);
        if (docPartCell == null) {
            docPartCell = new HashMap<>();
            docPartTable.put(tableRef.getParent().get(), name, docPartCell);
        }
        return docPartCell;
    }

    private List<KVValue<?>> getCellElements(Map<Integer, List<KVValue<?>>> docPartCell, Integer pid) {
        List<KVValue<?>> elements = docPartCell.get(pid);
        if (elements == null) {
            elements = new ArrayList<>();
            docPartCell.put(pid, elements);
        }
        return elements;
    }

    private void setElementValue(List<KVValue<?>> elements, Integer seq, KVValue<?> value) {
        final int size = elements.size();
        if (seq < size) {
            elements.set(seq, value);
        } else {
            for (int i=elements.size(); i<seq; i++) {
                elements.add(null);
            }
            elements.add(value);
        }
    }

}
