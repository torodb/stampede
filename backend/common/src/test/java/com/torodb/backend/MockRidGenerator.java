package com.torodb.backend;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.torodb.core.TableRef;

public class MockRidGenerator implements RidGenerator {
	
	private Map<String, AtomicInteger> rids=new HashMap<String, AtomicInteger>();

	@Override
	public int nextRid(String dbName, String collectionName, TableRef tableRef) {
		String key=dbName+"-"+collectionName+"-"+tableRef.toString();
		AtomicInteger rid = rids.computeIfAbsent(key, s -> new AtomicInteger(0));
		return rid.getAndIncrement();
	}
	

}
