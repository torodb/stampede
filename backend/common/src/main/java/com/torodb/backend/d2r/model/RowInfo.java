package com.torodb.backend.d2r.model;

import java.util.ArrayList;
import java.util.Iterator;

import com.torodb.core.d2r.DocPartData;
import com.torodb.core.d2r.DocPartRow;
import com.torodb.core.transaction.metainf.FieldType;
import com.torodb.kvdocument.values.KVArray;
import com.torodb.kvdocument.values.KVBoolean;
import com.torodb.kvdocument.values.KVDocument;
import com.torodb.kvdocument.values.KVValue;

public class RowInfo implements DocPartRow{

	private final int did;
	private final int rid;
	private final Integer pid;
	private final Integer seq;
	
	private final ArrayList<KVValue<?>> attributes;
	private final TableMetadata tableMetadata;
	private final TableInfo tableInfo;

	public RowInfo(TableMetadata tableMetadata, Integer seq, RowInfo parentRow, TableInfo tableInfo) {
		this.tableInfo=tableInfo;
		this.rid = tableMetadata.getNextRowId();
		this.seq = seq;
		if (parentRow==null){
			this.did = this.rid;
			this.pid = null;
		}else{
			this.did = parentRow.did;
			this.pid = parentRow.rid;
		}
		this.tableMetadata = tableMetadata;
		this.attributes=new ArrayList<KVValue<?>>(); //initialize with metadata current size?
	}

	//TODO: review name for array values
	private static final String ARRAY_VALUE_NAME = "v";
	private static final KVBoolean IS_ARRAY = KVBoolean.TRUE;
	private static final KVBoolean IS_SUBDOCUMENT = KVBoolean.FALSE;
	
	public void addScalar(String key, KVValue<?> value) {
		Integer position = findPosition(key, FieldType.from(value.getType()));
		attributes.set(position, value);
	}

	public void addChild(String key, KVValue<?> value) {
		Integer position = findPosition(key, FieldType.from(value.getType()));
		if (value instanceof KVArray){
			attributes.set(position, IS_ARRAY);
		}else if (value instanceof KVDocument){
			attributes.set(position, IS_SUBDOCUMENT);
		}else {
			throw new IllegalArgumentException("Child value is not KVArray or KVDocument");
		}
	}
	
	public void addArrayItem(KVValue<?> value) {
		addScalar(ARRAY_VALUE_NAME, value);
	}
	
	public void addChildToArray(KVValue<?> value){
		addChild(ARRAY_VALUE_NAME, value);
	}
	
	private Integer findPosition(String key, FieldType fieldType) {
		Integer position = tableMetadata.findPosition(key, fieldType);
		if (position>=attributes.size()){
			for(int i=attributes.size();i<=position;i++){
				attributes.add(null);
			}
		}
		return position;
	}

	@Override
	public DocPartData getDocPartData(){
		return tableInfo;
	}

	@Override
	public Integer getSeq() {
		return seq;
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
	public Iterator<KVValue<?>> iterator() {
		return attributes.iterator();
	}

}
