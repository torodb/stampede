package com.torodb.backend.util;

import java.util.concurrent.atomic.AtomicInteger;

import com.torodb.core.TableRef;
import com.torodb.core.d2r.RidGenerator;

public class InMemoryRidGenerator implements RidGenerator {
	
	private AtomicInteger global=new AtomicInteger(0);
	
	@Override
	public int nextRid(String dbName, String collectionName, TableRef tableRef) {
		return global.getAndIncrement();
	}

}
