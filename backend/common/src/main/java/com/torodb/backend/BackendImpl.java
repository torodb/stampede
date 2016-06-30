
package com.torodb.backend;

import com.google.common.util.concurrent.AbstractIdleService;
import com.torodb.backend.meta.SchemaUpdater;
import com.torodb.backend.meta.SnapshotUpdater;
import com.torodb.backend.rid.MaxRowIdFactory;
import com.torodb.core.TableRefFactory;
import com.torodb.core.backend.Backend;
import com.torodb.core.backend.BackendConnection;
import com.torodb.core.d2r.R2DTranslator;
import com.torodb.core.transaction.metainf.MetainfoRepository;
import java.sql.ResultSet;
import javax.inject.Inject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 */
public class BackendImpl extends AbstractIdleService implements Backend {

    private static final Logger LOGGER = LogManager.getLogger(BackendImpl.class);

    private final DbBackendService dbBackendService;
    private final SqlInterface sqlInterface;
    private final SqlHelper sqlHelper;
    private final SchemaUpdater schemaUpdater;
    private final MetainfoRepository metainfoRepository;
    private final TableRefFactory tableRefFactory;
    private final MaxRowIdFactory maxRowIdFactory;
    private final R2DTranslator<ResultSet> r2dTranslator;

    @Inject
    public BackendImpl(DbBackendService dbBackendService, SqlInterface sqlInterface, SqlHelper sqlHelper, SchemaUpdater schemaUpdater, 
            MetainfoRepository metainfoRepository, TableRefFactory tableRefFactory, MaxRowIdFactory maxRowIdFactory,
            R2DTranslator<ResultSet> r2dTranslator) {
        this.dbBackendService = dbBackendService;
        this.sqlInterface = sqlInterface;
        this.sqlHelper = sqlHelper;
        this.schemaUpdater = schemaUpdater;
        this.metainfoRepository = metainfoRepository;
        this.tableRefFactory = tableRefFactory;
        this.maxRowIdFactory = maxRowIdFactory;
        this.r2dTranslator = r2dTranslator;
    }
    @Override
    public BackendConnection openConnection() {
        return new BackendConnectionImpl(this, sqlInterface, r2dTranslator);
    }

    @Override
    protected void startUp() throws Exception {
        LOGGER.debug("Starting backend");
        LOGGER.trace("Starting backend datasources...");
        dbBackendService.startAsync();
        dbBackendService.awaitRunning();
        LOGGER.trace("Loading backend metadata...");
        SnapshotUpdater.updateSnapshot(metainfoRepository, sqlInterface, sqlHelper, schemaUpdater, tableRefFactory);
        LOGGER.trace("Reading last used rids...");
        maxRowIdFactory.startAsync();
        maxRowIdFactory.awaitRunning();
        LOGGER.debug("Backend ready to run");
    }

    @Override
    protected void shutDown() throws Exception {
    }

    void onConnectionClosed(BackendConnectionImpl connection) {
    }
}
