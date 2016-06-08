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

public class DocPartDataCollection implements CollectionData{

	private final Map<PathInfo, DocPartDataImpl> docPartDataMap = new HashMap<>();
	private final List<DocPartData> docPartDataList = new ArrayList<>();
	private final CollectionMetaInfo collectionMetaInfo;

	public DocPartDataCollection(CollectionMetaInfo collectionMetaInfo) {
		this.collectionMetaInfo = collectionMetaInfo;
	}

	public DocPartDataImpl findDocPartData(PathInfo path) {
		DocPartDataImpl docPartData = docPartDataMap.get(path);
		if (docPartData == null) {
			TableMetadata metadata=new TableMetadata(collectionMetaInfo, path.getTableRef());
			docPartData = new DocPartDataImpl(metadata);
			docPartDataMap.put(path, docPartData);
			docPartDataList.add(docPartData);
		}
		return docPartData;
	}

	@Override
	public Iterator<DocPartData> iterator() {
		return docPartDataList.iterator();
	}
}
