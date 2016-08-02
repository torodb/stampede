
package com.torodb.backend;

import com.torodb.backend.meta.SchemaUpdater;
import com.torodb.backend.meta.SnapshotUpdater;
import com.torodb.backend.rid.MaxRowIdFactory;
import com.torodb.common.util.ThreadFactoryIdleService;
import com.torodb.core.TableRefFactory;
import com.torodb.core.annotations.ToroDbIdleService;
import com.torodb.core.backend.Backend;
import com.torodb.core.backend.BackendConnection;
import com.torodb.core.d2r.IdentifierFactory;
import com.torodb.core.d2r.R2DTranslator;
import com.torodb.core.d2r.RidGenerator;
import com.torodb.core.transaction.metainf.MetainfoRepository;
import java.util.concurrent.ThreadFactory;
import javax.inject.Inject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 */
public class BackendImpl extends ThreadFactoryIdleService implements Backend {

    private static final Logger LOGGER = LogManager.getLogger(BackendImpl.class);

    private final DbBackendService dbBackendService;
    private final SqlInterface sqlInterface;
    private final SqlHelper sqlHelper;
    private final SchemaUpdater schemaUpdater;
    private final MetainfoRepository metainfoRepository;
    private final TableRefFactory tableRefFactory;
    private final MaxRowIdFactory maxRowIdFactory;
    private final R2DTranslator r2dTranslator;
    private final IdentifierFactory identifierFactory;
    private final RidGenerator ridGenerator;

    /**
     * @param threadFactory the thread factory that will be used to create the startup and shutdown
     *                      threads
     * @param dbBackendService
     * @param sqlInterface
     * @param sqlHelper
     * @param schemaUpdater
     * @param metainfoRepository
     * @param tableRefFactory
     * @param maxRowIdFactory
     * @param r2dTranslator
     * @param identifierFactory
     * @param ridGenerator
     */
    @Inject
    public BackendImpl(@ToroDbIdleService ThreadFactory threadFactory, DbBackendService dbBackendService, SqlInterface sqlInterface, SqlHelper sqlHelper, SchemaUpdater schemaUpdater,
            MetainfoRepository metainfoRepository, TableRefFactory tableRefFactory, MaxRowIdFactory maxRowIdFactory,
            R2DTranslator r2dTranslator, IdentifierFactory identifierFactory, RidGenerator ridGenerator) {
        super(threadFactory);
        this.dbBackendService = dbBackendService;
        this.sqlInterface = sqlInterface;
        this.sqlHelper = sqlHelper;
        this.schemaUpdater = schemaUpdater;
        this.metainfoRepository = metainfoRepository;
        this.tableRefFactory = tableRefFactory;
        this.maxRowIdFactory = maxRowIdFactory;
        this.r2dTranslator = r2dTranslator;
        this.identifierFactory = identifierFactory;
        this.ridGenerator = ridGenerator;
    }
    
    @Override
    public BackendConnection openConnection() {
        return new BackendConnectionImpl(this, sqlInterface, r2dTranslator, identifierFactory, ridGenerator);
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
        maxRowIdFactory.stopAsync();
        maxRowIdFactory.awaitTerminated();
        dbBackendService.stopAsync();
        dbBackendService.awaitTerminated();
    }

    void onConnectionClosed(BackendConnectionImpl connection) {
    }
}
