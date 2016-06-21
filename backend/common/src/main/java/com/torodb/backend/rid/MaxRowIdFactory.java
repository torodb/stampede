package com.torodb.backend.rid;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.inject.Inject;

import com.torodb.backend.meta.TorodbMeta;
import com.torodb.core.TableRef;

public class MaxRowIdFactory implements ReservedIdInfoFactory {

	private final Map<String, Map<String, Map<TableRef, ReservedIdInfo>>> megaMap;

	@Inject
	public MaxRowIdFactory(TorodbMeta torodbMeta) {
			this.megaMap = copyToConcurrentMap(torodbMeta.getLastIds());
	}

	private Map<String, Map<String, Map<TableRef, ReservedIdInfo>>> copyToConcurrentMap(Map<String, Map<String, Map<TableRef, Integer>>> map){
		ConcurrentHashMap<String, Map<String, Map<TableRef, ReservedIdInfo>>> concurrentDbMap = new ConcurrentHashMap<>();
		map.forEach((dbName, collMap)-> {
			ConcurrentHashMap<String, Map<TableRef, ReservedIdInfo>> concCollMap = new ConcurrentHashMap<>();
			concurrentDbMap.put(dbName, concCollMap);
			collMap.forEach((collName, trMap) -> {
				ConcurrentHashMap<TableRef,ReservedIdInfo> concTrMap = new ConcurrentHashMap<>();
				concCollMap.put(collName, concTrMap);
				trMap.forEach((tableRef, lastUsedId) ->{
					concTrMap.put(tableRef, new ReservedIdInfo(lastUsedId, lastUsedId));
				});
			});
		});
		return concurrentDbMap;
	}
	
	@Override
	public ReservedIdInfo create(String dbName, String collectionName, TableRef tableRef) {
		Map<String, Map<TableRef, ReservedIdInfo>> collectionsMap = this.megaMap.computeIfAbsent(dbName,
				name -> new HashMap<>());
		Map<TableRef, ReservedIdInfo> docPartsMap = collectionsMap.computeIfAbsent(collectionName,
				name -> new HashMap<>());
		return docPartsMap.computeIfAbsent(tableRef, tr -> new ReservedIdInfo(-1, -1));
	}

}
