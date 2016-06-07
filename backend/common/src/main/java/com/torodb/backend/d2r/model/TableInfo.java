package com.torodb.backend.d2r.model;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.torodb.core.d2r.DocPartData;
import com.torodb.core.d2r.DocPartRow;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.core.transaction.metainf.MetaField;

public class TableInfo implements DocPartData{

	private List<DocPartRow> rows = new ArrayList<>();
	private TableMetadata metadata;

	public TableInfo(TableMetadata metadata) {
		this.metadata = metadata;
	}

	public RowInfo newRowObject(Integer index, RowInfo parentRow) {
		RowInfo rowInfo = new RowInfo(metadata, index, parentRow, this);
		rows.add(rowInfo);
		return rowInfo;
	}

	public MetaDocPart getMetaDocPart(){
		return metadata.getMetaDocPart();
	}

	@Override
	public String toString() {
		return metadata.getMetaDocPart().getTableRef().toString();
	}

	@Override
	public Iterator<DocPartRow> iterator() {
		return rows.iterator();
	}

	@Override
	public int columnCount() {
		return metadata.getOrdererdFields().size();
	}

	@Override
	public int rowCount() {
		return rows.size();
	}

	@Override
	public Iterator<MetaField> orderedMetaFieldIterator() {
		return metadata.getOrdererdFields().iterator();
	}

}
