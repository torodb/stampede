package com.torodb.backend.d2r.model;

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.torodb.backend.d2r.CollectionMetaInfo;
import com.torodb.core.TableRef;
import com.torodb.core.transaction.metainf.FieldType;
import com.torodb.core.transaction.metainf.ImmutableMetaField;
import com.torodb.core.transaction.metainf.MetaField;
import com.torodb.core.transaction.metainf.MutableMetaDocPart;

public class TableMetadata {

	private final CollectionMetaInfo collectionMetaInfo;
	private final TableRef tableRef;
	private final MutableMetaDocPart metaDocPart;
	private final Table<String, FieldType, Integer> order;
	private final List<MetaField> orderedFields;

	public TableMetadata(CollectionMetaInfo collectionMetaInfo, TableRef tableRef) {
		this.collectionMetaInfo = collectionMetaInfo;
		this.tableRef = tableRef;
		this.metaDocPart = collectionMetaInfo.findMetaDocPart(tableRef);
		this.order = HashBasedTable.create();
		this.orderedFields = new ArrayList<>();
	}

	protected MutableMetaDocPart getMetaDocPart() {
		return metaDocPart;
	}

	protected List<MetaField> getOrdererdFields() {
		return orderedFields;
	}
	
	protected int getNextRowId(){
		return collectionMetaInfo.getNextRowId(tableRef);
	}

	protected Integer findPosition(String key, FieldType type) {
		Integer idx = order.get(key, type);
		if (idx == null) {
			idx = orderedFields.size();
			order.put(key, type, idx);
			orderedFields.add(findMetaField(key, type));
		}
		return idx;
	}

	private MetaField findMetaField(String key, FieldType type) {
		ImmutableMetaField metaField = metaDocPart.getMetaFieldByNameAndType(key, type);
		if (metaField == null) {
			String identifier = collectionMetaInfo.getFieldIdentifier(tableRef, type, key);
			metaField = metaDocPart.addMetaField(key, identifier, type);
		}
		return metaField;
	}

}