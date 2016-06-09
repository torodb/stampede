package com.torodb.backend.d2r.model;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.torodb.core.d2r.DocPartData;
import com.torodb.core.d2r.DocPartRow;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.core.transaction.metainf.MetaField;

public class DocPartDataImpl implements DocPartData{

	private List<DocPartRow> docPartRows = new ArrayList<>();
	private TableMetadata metadata;
	private DocPartDataImpl parent;
	private List<DocPartDataImpl> childs = null;

	public DocPartDataImpl(TableMetadata metadata, DocPartDataImpl parent) {
		this.metadata = metadata;
		this.parent= parent;
		if (parent!=null){
			this.parent.addChild(this);
		}
	}

	public DocPartRowImpl newRowObject(Integer index, DocPartRowImpl parentRow) {
		DocPartRowImpl docPartRow = new DocPartRowImpl(metadata, index, parentRow, this);
		docPartRows.add(docPartRow);
		return docPartRow;
	}

	public MetaDocPart getMetaDocPart(){
		return metadata.getMetaDocPart();
	}
	
	public DocPartDataImpl getParentDocPartRow() {
		return parent;
	}
	
	public List<DocPartDataImpl> getChilds(){
		return childs;
	}

	private void addChild(DocPartDataImpl child) {
		if (childs == null) {
			childs = new ArrayList<>();
		}
		childs.add(child);
	}
	
	@Override
	public String toString() {
		return metadata.getMetaDocPart().getTableRef().toString();
	}

	@Override
	public Iterator<DocPartRow> iterator() {
		return docPartRows.iterator();
	}

	@Override
	public int columnCount() {
		return metadata.getOrdererdFields().size();
	}

	@Override
	public int rowCount() {
		return docPartRows.size();
	}

	@Override
	public Iterator<MetaField> orderedMetaFieldIterator() {
		return metadata.getOrdererdFields().iterator();
	}

}
