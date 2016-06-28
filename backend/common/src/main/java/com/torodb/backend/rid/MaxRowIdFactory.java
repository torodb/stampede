package com.torodb.backend.rid;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractIdleService;
import com.torodb.backend.SqlInterface;
import com.torodb.core.TableRef;
import com.torodb.core.transaction.metainf.ImmutableMetaSnapshot;
import com.torodb.core.transaction.metainf.MetaSnapshot;
import com.torodb.core.transaction.metainf.MetainfoRepository;
import com.torodb.core.transaction.metainf.MetainfoRepository.SnapshotStage;
import java.sql.Connection;
import java.util.concurrent.ConcurrentHashMap;
import javax.inject.Inject;
import org.jooq.DSLContext;

public class MaxRowIdFactory extends AbstractIdleService implements ReservedIdInfoFactory {

    private final MetainfoRepository metainfoRepository;
    private final SqlInterface sqlInterface;
	private ConcurrentHashMap<String, ConcurrentHashMap<String, ConcurrentHashMap<TableRef, ReservedIdInfo>>> megaMap;

    @Inject
    public MaxRowIdFactory(MetainfoRepository metainfoRepository, SqlInterface sqlInterface) {
        this.metainfoRepository = metainfoRepository;
        this.sqlInterface = sqlInterface;
    }

    @Override
    protected void startUp() throws Exception {
        ImmutableMetaSnapshot snapshot;
        try (SnapshotStage snapshotStage = metainfoRepository.startSnapshotStage()) {
            snapshot = snapshotStage.createImmutableSnapshot();
        }

        try (Connection connection = sqlInterface.getDbBackend().createSystemConnection()) {
            DSLContext dsl = sqlInterface.getDslContextFactory().createDSLContext(connection);

            megaMap = loadRowIds(dsl, snapshot);
        }

    }

    @Override
    protected void shutDown() throws Exception {
    }

    private ConcurrentHashMap<String, ConcurrentHashMap<String, ConcurrentHashMap<TableRef, ReservedIdInfo>>> loadRowIds(DSLContext dsl, MetaSnapshot snapshot) {
        ConcurrentHashMap<String, ConcurrentHashMap<String, ConcurrentHashMap<TableRef, ReservedIdInfo>>> rowsIdMap = new ConcurrentHashMap<>();

        snapshot.streamMetaDatabases().forEach(db -> {
            ConcurrentHashMap<String, ConcurrentHashMap<TableRef, ReservedIdInfo>> collMap
                    = new ConcurrentHashMap<>();
            rowsIdMap.put(db.getName(), collMap);
            db.streamMetaCollections().forEach(collection -> {
                ConcurrentHashMap<TableRef, ReservedIdInfo> tableRefMap = new ConcurrentHashMap<>();
                collMap.put(collection.getName(), tableRefMap);
                collection.streamContainedMetaDocParts().forEach(metaDocPart -> {
                    TableRef tableRef = metaDocPart.getTableRef();
                    Integer lastRowIUsed = sqlInterface.getReadInterface().getLastRowIdUsed(dsl, db, collection, metaDocPart);
                    tableRefMap.put(tableRef, new ReservedIdInfo(lastRowIUsed, lastRowIUsed));
                });
            });
        });
        return rowsIdMap;
    }
	
	@Override
	public ReservedIdInfo create(String dbName, String collectionName, TableRef tableRef) {
        Preconditions.checkState(isRunning(), "This " + ReservedIdInfoFactory.class
                + " is also a service and it is not running");

        assert megaMap != null;

		ConcurrentHashMap<String, ConcurrentHashMap<TableRef, ReservedIdInfo>> collectionsMap = this.megaMap.computeIfAbsent(dbName,
				name -> new ConcurrentHashMap<>());
		ConcurrentHashMap<TableRef, ReservedIdInfo> docPartsMap = collectionsMap.computeIfAbsent(collectionName,
				name -> new ConcurrentHashMap<>());
		return docPartsMap.computeIfAbsent(tableRef, tr -> new ReservedIdInfo(-1, -1));
	}

}
