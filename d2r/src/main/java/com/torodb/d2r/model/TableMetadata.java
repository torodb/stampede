package com.torodb.d2r.model;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.torodb.core.TableRef;
import com.torodb.core.transaction.metainf.FieldType;
import com.torodb.core.transaction.metainf.MetaField;
import com.torodb.core.transaction.metainf.MetaScalar;
import com.torodb.core.transaction.metainf.MutableMetaDocPart;
import com.torodb.d2r.CollectionMetaInfo;

public class TableMetadata {

	private final CollectionMetaInfo collectionMetaInfo;
	private final TableRef tableRef;
	private final MutableMetaDocPart metaDocPart;
	
	private final Table<String, FieldType, Integer> fieldOrder;
	private final Map<FieldType, Integer> scalarOrder;
	
	private final List<MetaField> orderedFields;
	private final List<MetaScalar> orderedScalars;

	public TableMetadata(CollectionMetaInfo collectionMetaInfo, TableRef tableRef) {
		this.collectionMetaInfo = collectionMetaInfo;
		this.tableRef = tableRef;
		this.metaDocPart = collectionMetaInfo.findMetaDocPart(tableRef);
		this.fieldOrder = HashBasedTable.create();
		this.scalarOrder = new EnumMap<>(FieldType.class);
		this.orderedFields = new ArrayList<>();
		this.orderedScalars = new ArrayList<>();
	}

	protected MutableMetaDocPart getMetaDocPart() {
		return metaDocPart;
	}

	protected List<MetaField> getOrdererdFields() {
		return orderedFields;
	}
	
	protected List<MetaScalar> getOrdererdScalars() {
		return orderedScalars;
	}
	
	protected int getNextRowId(){
		return collectionMetaInfo.getNextRowId(tableRef);
	}

	protected Integer findFieldPosition(String key, FieldType type) {
		Integer idx = fieldOrder.get(key, type);
		if (idx == null) {
			idx = orderedFields.size();
			fieldOrder.put(key, type, idx);
			orderedFields.add(findMetaField(key, type));
		}
		return idx;
	}
	
	protected Integer findScalarPosition(FieldType type) {
		Integer idx = scalarOrder.get(type);
		if (idx == null) {
			idx = orderedScalars.size();
			scalarOrder.put(type, idx);
			orderedScalars.add(findMetaScalar(type));
		}
		return idx;
	}

	private MetaField findMetaField(String key, FieldType type) {
		MetaField metaField = metaDocPart.getMetaFieldByNameAndType(key, type);
		if (metaField == null) {
			String identifier = collectionMetaInfo.getFieldIdentifier(tableRef, type, key);
			metaField = metaDocPart.addMetaField(key, identifier, type);
		}
		return metaField;
	}
	
	private MetaScalar findMetaScalar(FieldType type) {
		MetaScalar metaField = metaDocPart.getScalar(type);
		if (metaField == null) {
			String identifier = collectionMetaInfo.getScalarIdentifier(type);
			metaField = metaDocPart.addMetaScalar(identifier, type);
		}
		return metaField;
	}

}