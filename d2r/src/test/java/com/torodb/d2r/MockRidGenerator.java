package com.torodb.d2r;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.torodb.core.TableRef;
import com.torodb.core.d2r.RidGenerator;

public class MockRidGenerator implements RidGenerator {
	
	private Table<String, String, DocPartRidGenerator> generators = HashBasedTable.create();

	@Override
	public int nextRid(String dbName, String collectionName, TableRef tableRef) {
		return getDocPartRidGenerator(dbName, collectionName).nextRid(tableRef);
	}

    @Override
    public void setNextRid(String dbName, String collectionName, TableRef tableRef, int nextRid) {
        getDocPartRidGenerator(dbName, collectionName).setNextRid(tableRef, nextRid);
    }
	
	@Override
	public DocPartRidGenerator getDocPartRidGenerator(String dbName, String collectionName) {
		DocPartRidGenerator map = generators.get(dbName, collectionName);
		if (map == null){
			map = new CollectionRidGeneratorMemory();
			generators.put(dbName, collectionName, map);
		}
		return map;
	}
	
	public static class CollectionRidGeneratorMemory implements DocPartRidGenerator {
		
		private Map<TableRef, AtomicInteger> map=new HashMap<TableRef, AtomicInteger>();

		@Override
		public int nextRid(TableRef tableRef) {
			AtomicInteger rid = map.computeIfAbsent(tableRef,tr -> new AtomicInteger(0));
			return rid.getAndIncrement();
		}

        @Override
        public void setNextRid(TableRef tableRef, int nextRid) {
            AtomicInteger rid = map.computeIfAbsent(tableRef,tr -> new AtomicInteger(0));
            rid.set(nextRid);
        }

	}

}
