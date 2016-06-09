package com.torodb.backend.d2r;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.torodb.backend.d2r.model.DocPartDataImpl;
import com.torodb.backend.d2r.model.PathStack.PathInfo;
import com.torodb.backend.d2r.model.TableMetadata;
import com.torodb.core.d2r.CollectionData;
import com.torodb.core.d2r.DocPartData;

public class DocPartDataCollection implements CollectionData {

	private final Map<PathInfo, DocPartDataImpl> docPartDataMap = new HashMap<>();
	private final List<DocPartDataImpl> docPartDataList = new ArrayList<>();
	private final CollectionMetaInfo collectionMetaInfo;

	public DocPartDataCollection(CollectionMetaInfo collectionMetaInfo) {
		this.collectionMetaInfo = collectionMetaInfo;
	}

	public DocPartDataImpl findDocPartData(PathInfo path) {
		DocPartDataImpl docPartData = docPartDataMap.get(path);
		if (docPartData == null) {
			TableMetadata metadata = new TableMetadata(collectionMetaInfo, path.getTableRef());
			DocPartDataImpl parentDocPartData = findParent(path);
			docPartData = new DocPartDataImpl(metadata, parentDocPartData);
			docPartDataMap.put(path, docPartData);
			docPartDataList.add(docPartData);
		}
		return docPartData;
	}

	private DocPartDataImpl findParent(PathInfo path) {
		PathInfo it = path.getParent();
		while (it != null) {
			DocPartDataImpl tableInfo = docPartDataMap.get(it);
			if (tableInfo != null) {
				return tableInfo;
			}
			it = it.getParent();
		}
		return null;
	}

	@Override
	public Iterator<DocPartData> iterator() {
		List<DocPartData> all = new ArrayList<>();
		for (DocPartDataImpl table: docPartDataList){
			if (table.getParentDocPartRow()==null){
				all.add(table);
				addChilds(table, all);
				return all.iterator();
			}
		}
		return all.iterator();
	}
	
	private void addChilds(DocPartDataImpl current, List<DocPartData> all) {
		List<DocPartDataImpl> childs = current.getChilds();
		if (childs!=null && childs.size() > 0) {
			all.addAll(childs);
			for (DocPartDataImpl child : childs) {
				addChilds(child, all);
			}
		}
	}
}
