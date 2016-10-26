package com.torodb.backend.util;

import java.util.concurrent.atomic.AtomicInteger;

import com.torodb.core.TableRef;
import com.torodb.core.annotations.TorodbIdleService;
import com.torodb.core.d2r.ReservedIdGenerator.DocPartRidGenerator;
import com.torodb.core.d2r.ReservedIdGenerator;
import com.torodb.core.services.IdleTorodbService;
import java.util.concurrent.ThreadFactory;

public class InMemoryRidGenerator extends IdleTorodbService implements ReservedIdGenerator, DocPartRidGenerator {
	
	private AtomicInteger global=new AtomicInteger(0);

    public InMemoryRidGenerator(@TorodbIdleService ThreadFactory threadFactory) {
        super(threadFactory);
    }
	
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

    @Override
    protected void startUp() throws Exception {
    }

    @Override
    protected void shutDown() throws Exception {
    }

}
