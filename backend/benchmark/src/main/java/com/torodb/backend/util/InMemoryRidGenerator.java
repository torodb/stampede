package com.torodb.backend.util;

import java.util.concurrent.atomic.AtomicInteger;

import com.torodb.core.TableRef;
import com.torodb.core.d2r.RidGenerator;
import com.torodb.core.d2r.RidGenerator.DocPartRidGenerator;

public class InMemoryRidGenerator implements RidGenerator, DocPartRidGenerator {
	
	private AtomicInteger global=new AtomicInteger(0);
	
	@Override
	public int nextRid(String dbName, String collectionName, TableRef tableRef) {
		return global.getAndIncrement();
	}

	@Override
	public DocPartRidGenerator getDocPartRidGenerator(String dbName, String collectionName) {
		return this;
	}

	@Override
	public int nextRid(TableRef tableRef) {
		return global.getAndIncrement();
	}

    @Override
    public void setNextRid(TableRef tableRef, int nextRid) {
        global.set(nextRid);
    }

    @Override
    public void setNextRid(String dbName, String collectionName, TableRef tableRef, int nextRid) {
        global.set(nextRid);
    }

}
