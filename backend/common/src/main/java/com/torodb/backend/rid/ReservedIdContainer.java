package com.torodb.backend.rid;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.inject.Inject;

import com.torodb.core.TableRef;
import com.torodb.core.d2r.RidGenerator;

public class ReservedIdContainer implements RidGenerator {

	private final Map<String, Map<String, Generator>> generators = new ConcurrentHashMap<>();
	private final ReservedIdInfoFactory factory;

	@Inject
	public ReservedIdContainer(ReservedIdInfoFactory factory) {
		this.factory = factory;
	}

	private Generator find(String dbName, String collectionName) {
		Map<String, Generator> collectionsMap = generators.computeIfAbsent(dbName,
				(key) -> new ConcurrentHashMap<>());
		return collectionsMap.computeIfAbsent(collectionName, (key) -> new Generator(dbName,collectionName));
	}

	@Override
	public int nextRid(String dbName, String collectionName, TableRef tableRef) {
		return getDocPartRidGenerator(dbName, collectionName).nextRid(tableRef);
	}

	@Override
	public DocPartRidGenerator getDocPartRidGenerator(String dbName, String collectionName) {
		return find(dbName, collectionName);
	}

	private class Generator implements DocPartRidGenerator {

		private String dbName;
		private String collectionName;
		private ConcurrentHashMap<TableRef, ReservedIdInfo> map = new ConcurrentHashMap<>();

		public Generator(String dbName, String collectionName) {
			this.dbName = dbName;
			this.collectionName = collectionName;
		}

		private ReservedIdInfo get(TableRef tableRef){
			return this.map.computeIfAbsent(tableRef, tr -> factory.create(dbName, collectionName, tr));
		}

		@Override
		public int nextRid(TableRef tableRef) {
			return get(tableRef).getAndAddLastUsedId(1) + 1;
		}

	}
}
