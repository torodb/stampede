package com.torodb.backend.d2r;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.torodb.backend.d2r.model.PathStack.PathInfo;
import com.google.common.collect.ImmutableList;
import com.torodb.backend.d2r.model.TableInfo;
import com.torodb.backend.d2r.model.TableMetadata;
import com.torodb.core.d2r.CollectionData;
import com.torodb.core.d2r.DocPartData;

public class TableRepository implements CollectionData{

	private final Map<PathInfo, TableInfo> tables = new HashMap<>();
	private final List<DocPartData> tablesList = new ArrayList<>();
	private final CollectionMetaInfo collectionMetaInfo;

	public TableRepository(CollectionMetaInfo collectionMetaInfo) {
		this.collectionMetaInfo = collectionMetaInfo;
	}

	public TableInfo findTable(PathInfo path) {
		TableInfo table = tables.get(path);
		if (table == null) {
			TableMetadata metadata=new TableMetadata(collectionMetaInfo, path.getTableRef());
			table = new TableInfo(metadata);
			tables.put(path, table);
			tablesList.add(table);
		}
		return table;
	}

	public List<DocPartData> getTables() {
		return ImmutableList.copyOf(tablesList);
	}

	@Override
	public Iterator<DocPartData> iterator() {
		return tablesList.iterator();
	}
}
