package com.torodb.d2r;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Table;
import com.torodb.core.TableRef;
import com.torodb.core.d2r.DocPartResult;
import com.torodb.core.d2r.DocPartResults;
import com.torodb.core.d2r.FieldValue;
import com.torodb.core.d2r.InternalFields;
import com.torodb.core.d2r.R2DBackendTranslator;
import com.torodb.core.d2r.R2DTranslator;
import com.torodb.core.transaction.BackendException;
import com.torodb.core.transaction.RollbackException;
import com.torodb.core.transaction.metainf.FieldType;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.core.transaction.metainf.MetaField;
import com.torodb.kvdocument.values.KVBoolean;
import com.torodb.kvdocument.values.KVDocument;
import com.torodb.kvdocument.values.KVValue;
import com.torodb.kvdocument.values.heap.ListKVArray;

public class R2DBackedTranslator<Result, BackendInternalFields extends InternalFields> implements R2DTranslator<Result> {
	
    private final R2DBackendTranslator<Result, BackendInternalFields> backendTranslator;
    
    public R2DBackedTranslator(R2DBackendTranslator<Result, BackendInternalFields> backendTranslator) {
        this.backendTranslator = backendTranslator;
	}

    @Override
    public Collection<KVDocument> translate(DocPartResults<Result> docPartResultSets) throws BackendException, RollbackException {
        ImmutableList.Builder<KVDocument> readedDocuments = ImmutableList.builder();
        Table<TableRef, String, Map<Integer, List<KVValue<?>>>> currentDocPartTable = 
                HashBasedTable.<TableRef, String, Map<Integer, List<KVValue<?>>>>create();
        Table<TableRef, String, Map<Integer, List<KVValue<?>>>> childDocPartTable = 
                HashBasedTable.<TableRef, String, Map<Integer, List<KVValue<?>>>>create();
        int previousDepth = -1;
        Iterator<DocPartResult<Result>> docPartResultSetIterator = docPartResultSets.iterator();
        while(docPartResultSetIterator.hasNext()) {
            DocPartResult<Result> docPartResultSet = docPartResultSetIterator.next();
            MetaDocPart metaDocPart = docPartResultSet.getMetaDocPart();
            TableRef tableRef = metaDocPart.getTableRef();
            
            if (previousDepth != -1 && previousDepth != tableRef.getDepth()) {
                Table<TableRef, String, Map<Integer, List<KVValue<?>>>> 
                    previousChildDocPartTable = childDocPartTable;
                childDocPartTable = currentDocPartTable;
                currentDocPartTable = previousChildDocPartTable;
                if (!tableRef.isRoot()) {
                    currentDocPartTable.clear();
                }
            }
            previousDepth = tableRef.getDepth();
            
            Map<String, Map<Integer, List<KVValue<?>>>> childDocPartRow = 
                    childDocPartTable.row(tableRef);
            
            Result result = docPartResultSet.getResult();
            
            readResult(metaDocPart, tableRef, result, currentDocPartTable, childDocPartRow, readedDocuments);
        }
        
        
        return readedDocuments.build();
    }
    
    protected void readResult(MetaDocPart metaDocPart, TableRef tableRef, Result result,
            Table<TableRef, String, Map<Integer, List<KVValue<?>>>> currentDocPartTable,
            Map<String, Map<Integer, List<KVValue<?>>>> childDocPartRow,
            ImmutableList.Builder<KVDocument> readedDocuments) throws BackendException, RollbackException {
        while (backendTranslator.next(result)) {
            BackendInternalFields internalFields = backendTranslator.readInternalFields(metaDocPart, result);
            Integer rid = internalFields.getRid();
            Integer pid = internalFields.getPid();
            Integer seq = internalFields.getSeq();
            
            KVDocument.Builder documentBuilder = new KVDocument.Builder();
            //TODO: ensure MetaField order using ResultSet meta data
            Iterator<? extends MetaField> metaFieldIterator = metaDocPart
                    .streamFields().iterator();
            boolean wasScalar = false;
            int fieldIndex = 0;
            while (metaFieldIterator.hasNext()) {
                MetaField metaField = metaFieldIterator.next();
                FieldValue fieldValue = backendTranslator.getFieldValue(metaField, result, internalFields, fieldIndex);
                fieldIndex++;
                
                if (fieldValue == FieldValue.NULL_VALUE) {
                    continue;
                }
                
                KVValue<?> value = fieldValue.getValue();
                
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
                
                if(metaField.getIdentifier().indexOf(backendTranslator.getScalarName()) == 0
                        && metaField.getIdentifier().length() == backendTranslator.getScalarName().length() + 2) {
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

    private Map<Integer, List<KVValue<?>>> getDocPartCell(
            Table<TableRef, String, Map<Integer, List<KVValue<?>>>> docPartTable, TableRef tableRef, Integer seq) {
        String name = tableRef.getName();
        
        if (seq != null && tableRef.isInArray()) {
            name = backendTranslator.getScalarName();
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
