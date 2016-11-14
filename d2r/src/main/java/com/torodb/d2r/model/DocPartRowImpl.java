/*
 * ToroDB - ToroDB-poc: D2R Implementation
 * Copyright © 2014 8Kdata Technology (www.8kdata.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.torodb.d2r.model;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;

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
	    final int position = findFieldPosition(key, FieldType.from(value.getType()));
		fieldAttributes.set(position, value);
	}

	public void addChild(String key, KVValue<?> value) {
	    final int position = findFieldPosition(key, FieldType.from(value.getType()));
		if (value instanceof KVArray){
			fieldAttributes.set(position, IS_ARRAY);
		} else if (value instanceof KVDocument) {
			fieldAttributes.set(position, IS_SUBDOCUMENT);
		} else {
			throw new IllegalArgumentException("Child value is not KVArray or KVDocument");
		}
	}
	
	public void addArrayItem(KVValue<?> value) {
		Integer position = findScalarPosition(FieldType.from(value.getType()));
		scalarAttributes.set(position, value);
	}
	
	public void addChildToArray(KVValue<?> value){
		final int position = findScalarPosition(FieldType.from(value.getType()));
		if (value instanceof KVArray){
			scalarAttributes.set(position, IS_ARRAY);
		} else if (value instanceof KVDocument) {
			scalarAttributes.set(position, IS_SUBDOCUMENT);
		} else {
			throw new IllegalArgumentException("Child value is not KVArray or KVDocument");
		}
	}
	
	private int findFieldPosition(String key, FieldType fieldType) {
		final int position = tableMetadata.findFieldPosition(key, fieldType);
		if (position >= fieldAttributes.size()) {
			for(int index = fieldAttributes.size(); index <= position; index++) {
				fieldAttributes.add(null);
			}
		}
		return position;
	}
	
	private int findScalarPosition(FieldType fieldType) {
		final int position = tableMetadata.findScalarPosition(fieldType);
		if (position >= scalarAttributes.size()) {
			for(int index = scalarAttributes.size(); index <= position; index++) {
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
		if (columns == attrs) {
			return fieldAttributes;
		}
		NumberNullIterator<KVValue<?>> itTail = new NumberNullIterator<>(columns-attrs);
		return () -> Iterators.concat(fieldAttributes.iterator(),itTail);
	}
	
	@Override
	public Iterable<KVValue<?>> getScalarValues() {
		int columns = this.getDocPartData().scalarColumnsCount();
		int attrs = this.scalarAttributes.size();
		if (columns == attrs) {
			return scalarAttributes;
		}
		NumberNullIterator<KVValue<?>> itTail = new NumberNullIterator<>(columns-attrs);
		return () -> Iterators.concat(scalarAttributes.iterator(),itTail);
	}
	
	private static class NumberNullIterator<R> implements Iterator<R> {

		private final int n;
		
		private int idx;
		
		public NumberNullIterator(int n) {
			this.n = n;
			this.idx = 0;
		}
		
		@Override
		public boolean hasNext() {
			return idx < n;
		}

		@Override
		public R next() {
			if (hasNext()){
				idx++;
				return null;
			}
			throw new NoSuchElementException();
		}
		
	}

}
