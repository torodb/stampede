package com.torodb.d2r.model;

import java.util.ArrayList;
import java.util.Iterator;

import com.google.common.collect.Iterators;
import com.torodb.core.d2r.DocPartData;
import com.torodb.core.d2r.DocPartRow;
import com.torodb.core.d2r.InternalFields;
import com.torodb.core.transaction.metainf.FieldType;
import com.torodb.kvdocument.values.KVArray;
import com.torodb.kvdocument.values.KVBoolean;
import com.torodb.kvdocument.values.KVDocument;
import com.torodb.kvdocument.values.KVValue;

public class DocPartRowImpl implements DocPartRow{

	private final int did;
	private final int rid;
	private final Integer pid;
	private final Integer seq;
	
	private final ArrayList<KVValue<?>> fieldAttributes;
	private final ArrayList<KVValue<?>> scalarAttributes;
	private final TableMetadata tableMetadata;
	private final DocPartDataImpl tableInfo;

	public DocPartRowImpl(TableMetadata tableMetadata, Integer seq, DocPartRowImpl parentRow, DocPartDataImpl tableInfo) {
		this.tableInfo = tableInfo;
		this.rid = tableMetadata.getNextRowId();
		this.seq = seq;
		if (parentRow == null) {
			this.did = this.rid;
			this.pid = null;
		} else {
			this.did = parentRow.did;
			this.pid = parentRow.rid;
		}
		this.tableMetadata = tableMetadata;
		this.fieldAttributes = new ArrayList<KVValue<?>>(); //initialize with metadata current size?
		this.scalarAttributes = new ArrayList<KVValue<?>>();
	}

	private static final KVBoolean IS_ARRAY = KVBoolean.from(InternalFields.CHILD_ARRAY_VALUE);
	private static final KVBoolean IS_SUBDOCUMENT = KVBoolean.from(InternalFields.CHILD_OBJECT_VALUE);
	
	public void addScalar(String key, KVValue<?> value) {
		Integer position = findFieldPosition(key, FieldType.from(value.getType()));
		fieldAttributes.set(position, value);
	}

	public void addChild(String key, KVValue<?> value) {
		Integer position = findFieldPosition(key, FieldType.from(value.getType()));
		if (value instanceof KVArray){
			fieldAttributes.set(position, IS_ARRAY);
		}else if (value instanceof KVDocument){
			fieldAttributes.set(position, IS_SUBDOCUMENT);
		}else {
			throw new IllegalArgumentException("Child value is not KVArray or KVDocument");
		}
	}
	
	public void addArrayItem(KVValue<?> value) {
		Integer position = findScalarPosition(FieldType.from(value.getType()));
		scalarAttributes.set(position, value);
	}
	
	public void addChildToArray(KVValue<?> value){
		Integer position = findScalarPosition(FieldType.from(value.getType()));
		if (value instanceof KVArray){
			scalarAttributes.set(position, IS_ARRAY);
		}else if (value instanceof KVDocument){
			scalarAttributes.set(position, IS_SUBDOCUMENT);
		}else {
			throw new IllegalArgumentException("Child value is not KVArray or KVDocument");
		}
	}
	
	private Integer findFieldPosition(String key, FieldType fieldType) {
		Integer position = tableMetadata.findFieldPosition(key, fieldType);
		if (position>=fieldAttributes.size()){
			for(int i=fieldAttributes.size();i<=position;i++){
				fieldAttributes.add(null);
			}
		}
		return position;
	}
	
	private Integer findScalarPosition(FieldType fieldType) {
		Integer position = tableMetadata.findScalarPosition(fieldType);
		if (position>=scalarAttributes.size()){
			for(int i=scalarAttributes.size();i<=position;i++){
				scalarAttributes.add(null);
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
	public Iterable<KVValue<?>> getFieldValues() {
		int columns = this.getDocPartData().fieldColumnsCount();
		int attrs = this.fieldAttributes.size();
		if (columns==attrs){
			return fieldAttributes;
		}
		NumberNullIterator<KVValue<?>> itTail=new NumberNullIterator<>(columns-attrs);
		return () -> Iterators.concat(fieldAttributes.iterator(),itTail);
	}
	
	@Override
	public Iterable<KVValue<?>> getScalarValues() {
		int columns = this.getDocPartData().scalarColumnsCount();
		int attrs = this.scalarAttributes.size();
		if (columns==attrs){
			return scalarAttributes;
		}
		NumberNullIterator<KVValue<?>> itTail=new NumberNullIterator<>(columns-attrs);
		return () -> Iterators.concat(scalarAttributes.iterator(),itTail);
	}
	
	private static class NumberNullIterator<R> implements Iterator<R>{

		private int n;
		private int idx;
		
		public NumberNullIterator(int n){
			this.n=n;
			this.idx=0;
		}
		
		@Override
		public boolean hasNext() {
			return idx<n;
		}

		@Override
		public R next() {
			idx++;
			return null;
		}
		
	}

}
