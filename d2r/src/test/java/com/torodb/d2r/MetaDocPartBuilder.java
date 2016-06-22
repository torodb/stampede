package com.torodb.d2r;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.torodb.core.TableRef;
import com.torodb.core.transaction.metainf.FieldType;
import com.torodb.core.transaction.metainf.ImmutableMetaDocPart;
import com.torodb.core.transaction.metainf.ImmutableMetaField;
import com.torodb.core.transaction.metainf.ImmutableMetaScalar;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.core.transaction.metainf.MetaField;
import com.torodb.core.transaction.metainf.MetaScalar;
import com.torodb.d2r.MockedResultSet.MockedRow;

public class MetaDocPartBuilder {

	private ImmutableMetaDocPart.Builder builder;
	private List<Object> both = new ArrayList<>();
	private MetaDocPart metaDocPart;
	private List<MockedRow> rows = new ArrayList<>();
	
	public MetaDocPartBuilder(TableRef tableRef) {
		builder = new ImmutableMetaDocPart.Builder(tableRef, tableRef.getName());
	}

	public void addMetaField(String name, String identifier, FieldType type){
		ImmutableMetaField metaField = new ImmutableMetaField(name, identifier, type);
		builder.add(metaField);
		both.add(metaField);
	}
	
	public void addMetaScalar(String identifier, FieldType type){
		ImmutableMetaScalar mestaScalar = new ImmutableMetaScalar(identifier, type);
		builder.add(mestaScalar);
		both.add(mestaScalar);
	}
	
	public MetaDocPart buildMetaDocPart(){
		metaDocPart = builder.build();
		return metaDocPart;
	}
	
	public MockedRow addRow(Integer did, Integer pid, Integer rid, Integer seq, Object... values){
		Object[] copy = new Object[values.length];
		for (int i = 0; i < values.length; i++) {
			int idx = findOrder(both.get(i));
			copy[idx] = values[i];
		}
		MockedRow row = new MockedRow(did, pid, rid, seq, copy);
		rows.add(row);
		return row;
	}
	
	public MockedResultSet getResultSet(){
		return new MockedResultSet(rows);
	}
	
	private int findOrder(Object field ){
		int idx = 0;
		Iterator<? extends MetaScalar> itScalar = metaDocPart.streamScalars().iterator();
		while (itScalar.hasNext()){
			MetaScalar next = itScalar.next();
			if (next==field){
				return idx;
			}
			idx++;
		}
		Iterator<? extends MetaField> it = metaDocPart.streamFields().iterator();
		while (it.hasNext()){
			MetaField next = it.next();
			if (next==field){
				return idx;
			}
			idx++;
		}
		throw new RuntimeException("Field not found");
	}

}
