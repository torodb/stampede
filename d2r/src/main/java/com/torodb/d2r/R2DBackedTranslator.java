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
import com.torodb.core.document.ToroDocument;
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
    public Collection<ToroDocument> translate(DocPartResults<Result> docPartResults) {
        ImmutableList.Builder<ToroDocument> readedDocuments = ImmutableList.builder();
        Table<TableRef, Integer, Map<String, List<KVValue<?>>>> currentDocPartTable = 
                HashBasedTable.<TableRef, Integer, Map<String, List<KVValue<?>>>>create();
        Table<TableRef, Integer, Map<String, List<KVValue<?>>>> childDocPartTable = 
                HashBasedTable.<TableRef, Integer, Map<String, List<KVValue<?>>>>create();
        int previousDepth = -1;
        Iterator<DocPartResult<Result>> docPartResultIterator = docPartResults.iterator();
        while(docPartResultIterator.hasNext()) {
            DocPartResult<Result> docPartResultSet = docPartResultIterator.next();
            MetaDocPart metaDocPart = docPartResultSet.getMetaDocPart();
            TableRef tableRef = metaDocPart.getTableRef();
            
            if (previousDepth != -1 && previousDepth != tableRef.getDepth()) {
                Table<TableRef, Integer, Map<String, List<KVValue<?>>>> 
                    previousChildDocPartTable = childDocPartTable;
                childDocPartTable = currentDocPartTable;
                currentDocPartTable = previousChildDocPartTable;
                if (!tableRef.isRoot()) {
                    currentDocPartTable.clear();
                }
            }
            previousDepth = tableRef.getDepth();
            
            Map<Integer, Map<String, List<KVValue<?>>>> childDocPartRow = 
                    childDocPartTable.row(tableRef);
            Map<Integer, Map<String, List<KVValue<?>>>> currentDocPartRow;
            
            if (tableRef.isRoot()) {
                currentDocPartRow = null;
            } else {
                currentDocPartRow = currentDocPartTable.row(tableRef.getParent().get());
            }
            
            Result result = docPartResultSet.getResult();
            
            readResult(metaDocPart, tableRef, result, currentDocPartRow, childDocPartRow, readedDocuments);
        }
        
        
        return readedDocuments.build();
    }
    
    private void readResult(MetaDocPart metaDocPart, TableRef tableRef, Result result,
            Map<Integer, Map<String, List<KVValue<?>>>> currentDocPartRow,
            Map<Integer, Map<String, List<KVValue<?>>>> childDocPartRow,
            ImmutableList.Builder<ToroDocument> readedDocuments) {
        KVDocument.Builder documentBuilder = new KVDocument.Builder();
        while (backendTranslator.next(result)) {
            BackendInternalFields internalFields = backendTranslator.readInternalFields(metaDocPart, result);
            Integer did = internalFields.getDid();
            Integer rid = internalFields.getRid();
            Integer pid = internalFields.getPid();
            Integer seq = internalFields.getSeq();
            
            Map<String, List<KVValue<?>>> childDocPartCell = childDocPartRow.get(rid);
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
                    value = getChildValue(value, metaField, childDocPartCell);
                }
                
                if(metaField.getIdentifier().indexOf(backendTranslator.getScalarName()) == 0
                        && metaField.getIdentifier().length() == backendTranslator.getScalarName().length() + 2) {
                    assert !tableRef.isRoot() : "found scalar value in root doc part";
                    addValueToDocPartRow(currentDocPartRow, tableRef, pid, seq, value);
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
                readedDocuments.add(new ToroDocument(did, documentBuilder.build()));
            } else {
                addValueToDocPartRow(currentDocPartRow, tableRef, pid, seq, documentBuilder.build());
            }
            
            documentBuilder = new KVDocument.Builder();
        }
    }

    private KVValue<?> getChildValue(KVValue<?> value, MetaField metaField,
            Map<String, List<KVValue<?>>> childDocPartCell) {
        KVBoolean child = (KVBoolean) value;
        if (child.getValue()) {
            List<KVValue<?>> elements;
            if (childDocPartCell == null || (elements = childDocPartCell.get(metaField.getName())) == null) {
                value = new ListKVArray(ImmutableList.of());
            } else {
                value = new ListKVArray(elements);
            }
        } else {
            value = childDocPartCell.get(metaField.getName()).get(0);
        }
        return value;
    }

    private void addValueToDocPartRow(Map<Integer, Map<String, List<KVValue<?>>>> currentDocPartRow, TableRef tableRef,
            Integer pid, Integer seq, KVValue<?> value) {
        if (seq == null) {
            setDocPartRowValue(currentDocPartRow, tableRef, pid, seq, ImmutableList.of(value));
        } else {
            addToDocPartRow(currentDocPartRow, tableRef, pid, seq, value);
        }
    }

    private void setDocPartRowValue(
            Map<Integer, Map<String, List<KVValue<?>>>> docPartRow, TableRef tableRef, Integer pid, Integer seq, ImmutableList<KVValue<?>> elements) {
        String name = getDocPartCellName(tableRef, seq);
        
        Map<String, List<KVValue<?>>> docPartCell = getDocPartCell(docPartRow, pid);
        
        docPartCell.put(name, elements);
    }

    private void addToDocPartRow(
            Map<Integer, Map<String, List<KVValue<?>>>> docPartRow, TableRef tableRef, Integer pid, Integer seq, KVValue<?> value) {
        String name = getDocPartCellName(tableRef, seq);
        
        Map<String, List<KVValue<?>>> docPartCell = getDocPartCell(docPartRow, pid);
        
        List<KVValue<?>> elements = docPartCell.get(name);
        if (elements == null) {
            elements = new ArrayList<>();
            docPartCell.put(name, elements);
        }
        
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

    private String getDocPartCellName(TableRef tableRef, Integer seq) {
        String name = tableRef.getName();
        
        if (seq != null && tableRef.isInArray()) {
            name = backendTranslator.getScalarName();
        }
        return name;
    }

    private Map<String, List<KVValue<?>>> getDocPartCell(Map<Integer, Map<String, List<KVValue<?>>>> docPartRow,
            Integer pid) {
        Map<String, List<KVValue<?>>> docPartCell = 
                docPartRow.get(pid);
        if (docPartCell == null) {
            docPartCell = new HashMap<>();
            docPartRow.put(pid, docPartCell);
        }
        return docPartCell;
    }
}
