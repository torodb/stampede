
package com.torodb.backend;

import com.google.common.util.concurrent.AbstractIdleService;
import com.torodb.backend.meta.SnapshotUpdater;
import com.torodb.backend.rid.MaxRowIdFactory;
import com.torodb.core.TableRefFactory;
import com.torodb.core.backend.Backend;
import com.torodb.core.backend.BackendConnection;
import com.torodb.core.transaction.metainf.MetainfoRepository;
import javax.inject.Inject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 */
public class BackendImpl extends AbstractIdleService implements Backend {

    private static final Logger LOGGER = LogManager.getLogger(BackendImpl.class);

    private final SqlInterface sqlInterface;
    private final MetainfoRepository metainfoRepository;
    private final TableRefFactory tableRefFactory;
    private final MaxRowIdFactory maxRowIdFactory;

    @Inject
    public BackendImpl(SqlInterface sqlInterface, MetainfoRepository metainfoRepository, TableRefFactory tableRefFactory, MaxRowIdFactory maxRowIdFactory) {
        this.sqlInterface = sqlInterface;
        this.metainfoRepository = metainfoRepository;
        this.tableRefFactory = tableRefFactory;
        this.maxRowIdFactory = maxRowIdFactory;
    }

    @Override
    public BackendConnection openConnection() {
        return new BackendConnectionImpl(this, sqlInterface);
    }

    @Override
    protected void startUp() throws Exception {
        LOGGER.debug("Starting backend");
        LOGGER.trace("Loading backend metadata...");
        SnapshotUpdater.updateSnapshot(metainfoRepository, sqlInterface, tableRefFactory);
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
