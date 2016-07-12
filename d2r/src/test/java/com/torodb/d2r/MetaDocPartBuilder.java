package com.torodb.d2r;

import com.torodb.core.TableRef;
import com.torodb.core.transaction.metainf.*;
import com.torodb.d2r.MockedDocPartResult.MockedRow;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class MetaDocPartBuilder {

	private ImmutableMetaDocPart.Builder builder;
	private List<Object> both = new ArrayList<>();
	private MetaDocPart metaDocPart;
	private boolean metaDocPartBuilded = false;
	private List<MockedRow> rows = new ArrayList<>();
	
	public MetaDocPartBuilder(TableRef tableRef) {
		builder = new ImmutableMetaDocPart.Builder(tableRef, tableRef.getName());
	}

	public void addMetaField(String name, String identifier, FieldType type){
		ImmutableMetaField metaField = new ImmutableMetaField(name, identifier, type);
		builder.put(metaField);
		both.add(metaField);
	}
	
	public void addMetaScalar(String identifier, FieldType type){
		ImmutableMetaScalar mestaScalar = new ImmutableMetaScalar(identifier, type);
		builder.put(mestaScalar);
		both.add(mestaScalar);
	}
	
	private void buildMetaDocPart(){
		if (!metaDocPartBuilded){
			metaDocPart = builder.build();
			metaDocPartBuilded = true;
		}
	}
	
	public MockedRow addRow(Integer did, Integer pid, Integer rid, Integer seq, Object... values){
		buildMetaDocPart();
		Object[] copy = new Object[values.length];
		for (int i = 0; i < values.length; i++) {
			int idx = findOrder(both.get(i));
			copy[idx] = values[i];
		}
		MockedRow row = new MockedRow(did, pid, rid, seq, copy);
		rows.add(row);
		return row;
	}
	
	public MockedDocPartResult getResultSet(){
		buildMetaDocPart();
		return new MockedDocPartResult(metaDocPart, rows);
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
