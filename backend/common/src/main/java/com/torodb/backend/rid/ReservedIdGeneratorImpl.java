package com.torodb.backend.rid;

import com.torodb.core.TableRef;
import com.torodb.core.annotations.TorodbIdleService;
import com.torodb.core.d2r.ReservedIdGenerator;
import com.torodb.core.services.IdleTorodbService;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;
import javax.inject.Inject;

public class ReservedIdGeneratorImpl extends IdleTorodbService implements ReservedIdGenerator {

	private final Map<String, Map<String, Generator>> generators = new ConcurrentHashMap<>();
	private final ReservedIdInfoFactory factory;

    @Inject
    public ReservedIdGeneratorImpl(ReservedIdInfoFactory factory,
            @TorodbIdleService ThreadFactory threadFactory) {
        super(threadFactory);
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
    public void setNextRid(String dbName, String collectionName, TableRef tableRef, int nextRid) {
        getDocPartRidGenerator(dbName, collectionName).setNextRid(tableRef, nextRid);
    }

	@Override
	public DocPartRidGenerator getDocPartRidGenerator(String dbName, String collectionName) {
		return find(dbName, collectionName);
	}

    @Override
    protected void startUp() throws Exception {
        factory.startAsync();
        factory.awaitRunning();
    }

    @Override
    protected void shutDown() throws Exception {
        factory.stopAsync();
        factory.awaitTerminated();
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

        @Override
        public void setNextRid(TableRef tableRef, int nextRid) {
            get(tableRef).setLastUsedId(nextRid);
        }

	}
}
