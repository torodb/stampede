package com.torodb.backend;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import javax.inject.Provider;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.torodb.backend.AttributeReference.ArrayKey;
import com.torodb.backend.AttributeReference.Key;
import com.torodb.backend.AttributeReference.ObjectKey;
import com.torodb.backend.D2RVisitor.Callback;
import com.torodb.core.TableRef;
import com.torodb.core.d2r.CollectionData;
import com.torodb.core.d2r.DocPartData;
import com.torodb.core.transaction.metainf.FieldType;
import com.torodb.core.transaction.metainf.MetaField;
import com.torodb.core.transaction.metainf.MutableMetaCollection;
import com.torodb.core.transaction.metainf.MutableMetaDocPart;
import com.torodb.kvdocument.values.KVArray;
import com.torodb.kvdocument.values.KVBoolean;
import com.torodb.kvdocument.values.KVDocument;
import com.torodb.kvdocument.values.KVDocument.DocEntry;
import com.torodb.kvdocument.values.KVValue;

public class D2RVisitorCallbackImpl implements Callback<DocPartRowImpl>, CollectionData {
    private final MutableMetaCollection metaCollection;
    private final Provider<DocPartRidGenerator> docPartRidGeneratorProvider;
    private final AttributeReferenceTranslator attributeReferenceTranslator = new AttributeReferenceTranslator();
    private final Map<TableRef, DocPartDataImpl> docPartDataMap =
            Maps.newHashMap();
    
    public D2RVisitorCallbackImpl(MutableMetaCollection metaCollection, Provider<DocPartRidGenerator> docPartRidGeneratorProvider) {
        super();
        this.metaCollection = metaCollection;
        this.docPartRidGeneratorProvider = docPartRidGeneratorProvider;
    }
    
    @Override
    public Iterator<DocPartData> iterator() {
        return new Iterator<DocPartData>() {
            private final Iterator<Entry<TableRef, DocPartDataImpl>> iterator = docPartDataMap.entrySet().iterator();
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public DocPartData next() {
                return iterator.next().getValue();
            }
        };
    }
    
    public DocPartRowImpl visit(KVDocument document, AttributeReference attributeReference, TableRef tableRef,
            DocPartRowImpl parentRow) {
        DocPartDataImpl docPartData = getDocPartData(attributeReference, tableRef);
        
        DocPartRowImpl newRow = appendObjectRow(docPartData, parentRow, attributeReference);
        
        for (DocEntry<?> entry : document) {
            String key = entry.getKey();
            KVValue<?> value = entry.getValue();
            if (value instanceof KVDocument) {
                appendChildToObject(docPartData, newRow, attributeReference, key, KVBoolean.FALSE);
            } else if (value instanceof KVArray) {
                appendChildToObject(docPartData, newRow, attributeReference, key, KVBoolean.TRUE);
            } else {
                appendValueToObject(docPartData, newRow, attributeReference, key, value);
            }
        }
        
        return newRow;
    }
    
    public void visit(KVArray array, AttributeReference attributeReference, TableRef tableRef,
            DocPartRowImpl parentRow) {
        if (array.isEmpty()) {
            return;
        }
        
        attributeReference = attributeReference.append(new ArrayKey(0));
        tableRef = attributeReferenceTranslator.toTableRef(attributeReference);
        DocPartDataImpl docPartData = getDocPartData(attributeReference, tableRef);
        
        int index = 0;
        for (KVValue<?> element : array) {
            if (element instanceof KVDocument 
                    || element instanceof KVArray) {
                index++;
                continue;
            }
            
            appendRowWithValueToArray(docPartData, parentRow, index, element);
            index++;
        }
    }

    @Override
    public DocPartRowImpl visitArrayElement(KVArray array, AttributeReference attributeReference, TableRef tableRef,
            int index, DocPartRowImpl parentRow) {
        DocPartDataImpl docPartData = getDocPartData(attributeReference, tableRef);
        return appendRowWithChildToArray(docPartData, parentRow, index, KVBoolean.TRUE);
    }

    private DocPartDataImpl getDocPartData(AttributeReference attributeReference, TableRef tableRef) {
        DocPartDataImpl docPartData = docPartDataMap.get(tableRef);
        if (docPartData == null) {
            MutableMetaDocPart metaDocPart = metaCollection.getMetaDocPartByTableRef(tableRef);
            if (metaDocPart == null) {
                String identifier = attributeReferenceTranslator.toTableName(attributeReference, metaCollection.getName());
                metaDocPart = metaCollection.addMetaDocPart(tableRef, identifier);
            }
            docPartData = new DocPartDataImpl(metaDocPart, docPartRidGeneratorProvider.get());
            docPartDataMap.put(tableRef, docPartData);
        }
        return docPartData;
    }
    
    private DocPartRowImpl appendObjectRow(DocPartDataImpl docPartData, DocPartRowImpl parentRow, AttributeReference attributeReference) {
        TableRef tableRef = docPartData.getMetaDocPart().getTableRef();
        DocPartRowImpl newRow;
        
        if (!tableRef.getParent().isPresent()) {
            newRow = docPartData.appendRootRow();
        } else {
            Key<?> key = attributeReference.getLastKey();
            if (key instanceof ObjectKey) {
                newRow = docPartData.appendObjectRow(parentRow.getDid(), parentRow.getRid());
            } else {
                newRow = docPartData.appendArrayRow(parentRow.getDid(), parentRow.getRid(), ((ArrayKey) key).getIndex());
            }
        }
        
        return newRow;
    }
    
    private void appendValueToObject(DocPartDataImpl docPartData, DocPartRowImpl parentRow, AttributeReference attributeReference, String key, KVValue<?> value) {
        FieldType fieldType = FieldType.from(value.getType());
        MutableMetaDocPart metaDocPart = docPartData.getMutableMetaDocPart();
        MetaField metaField = getMetaField(metaDocPart, fieldType, key, attributeReference.append(new ObjectKey(key)));
        docPartData.appendColumnValue(parentRow, key, metaField.getIdentifier(), fieldType, value);
    }

    private void appendChildToObject(DocPartDataImpl docPartData, DocPartRowImpl row, AttributeReference attributeReference, String key, KVBoolean value) {
        MetaField metaField = getMetaField(docPartData.getMutableMetaDocPart(), FieldType.CHILD, key, attributeReference.append(new ObjectKey(key)));
        docPartData.appendColumnValue(row, key, metaField.getIdentifier(), FieldType.CHILD, value);
    }
    
    private static final String scalarValueKey = "v";
    private static final AttributeReference scalarAttributeReference = new AttributeReference(ImmutableList.of(new ObjectKey(scalarValueKey)));
    
    private DocPartRowImpl appendRowWithValueToArray(DocPartDataImpl docPartData, DocPartRowImpl parentRow, int index, KVValue<?> value) {
        FieldType fieldType = FieldType.from(value.getType());
        MutableMetaDocPart metaDocPart = docPartData.getMutableMetaDocPart();
        MetaField metaField = getMetaField(metaDocPart, fieldType, scalarValueKey, scalarAttributeReference);
        DocPartRowImpl newRow = docPartData.appendArrayRow(parentRow.getDid(), parentRow.getRid(), index);
        docPartData.appendColumnValue(newRow, scalarValueKey, metaField.getIdentifier(), fieldType, value);
        return newRow;
    }
    
    private DocPartRowImpl appendRowWithChildToArray(DocPartDataImpl docPartData, DocPartRowImpl parentRow, int index, KVBoolean value) {
        MetaField metaField = getMetaField(docPartData.getMutableMetaDocPart(), FieldType.CHILD, scalarValueKey, scalarAttributeReference);
        DocPartRowImpl newRow = docPartData.appendArrayRow(parentRow.getDid(), parentRow.getRid(), index);
        docPartData.appendColumnValue(newRow, scalarValueKey, metaField.getIdentifier(), FieldType.CHILD, value);
        return newRow;
    }

    private MetaField getMetaField(MutableMetaDocPart metaDocPart, FieldType fieldType, String key, AttributeReference attributeReference) {
        MetaField metaField = metaDocPart.getMetaFieldByNameAndType(key, fieldType);
        if (metaField == null) {
            String identifier = attributeReferenceTranslator.toColumnName(attributeReference, fieldType);
            metaField = metaDocPart.addMetaField(key, identifier, fieldType);
        }
        return metaField;
    }
}
