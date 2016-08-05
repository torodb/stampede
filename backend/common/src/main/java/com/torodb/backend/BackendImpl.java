
package com.torodb.backend;

import com.torodb.backend.ErrorHandler.Context;
import com.torodb.backend.meta.SchemaUpdater;
import com.torodb.backend.meta.SnapshotUpdater;
import com.torodb.backend.rid.MaxRowIdFactory;
import com.torodb.common.util.ThreadFactoryIdleService;
import com.torodb.core.TableRefFactory;
import com.torodb.core.annotations.ToroDbIdleService;
import com.torodb.core.backend.Backend;
import com.torodb.core.backend.BackendConnection;
import com.torodb.core.concurrent.StreamExecutor;
import com.torodb.core.d2r.IdentifierFactory;
import com.torodb.core.d2r.R2DTranslator;
import com.torodb.core.d2r.RidGenerator;
import com.torodb.core.exceptions.ToroRuntimeException;
import com.torodb.core.retrier.Retrier;
import com.torodb.core.retrier.RetrierGiveUpException;
import com.torodb.core.transaction.RollbackException;
import com.torodb.core.transaction.metainf.*;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ThreadFactory;
import java.util.function.Consumer;
import java.util.stream.Stream;
import javax.inject.Inject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;

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
    private final Retrier retrier;
    private final StreamExecutor streamExecutor;

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
     * @param retrier
     * @param executor
     */
    @Inject
    public BackendImpl(@ToroDbIdleService ThreadFactory threadFactory, RidGenerator ridGenerator,
            DbBackendService dbBackendService, SqlInterface sqlInterface, SqlHelper sqlHelper,
            SchemaUpdater schemaUpdater, MetainfoRepository metainfoRepository,
            TableRefFactory tableRefFactory, MaxRowIdFactory maxRowIdFactory,
            R2DTranslator r2dTranslator, IdentifierFactory identifierFactory, Retrier retrier,
            StreamExecutor streamExecutor) {
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
        this.retrier = retrier;
        this.streamExecutor = streamExecutor;
    }
    
    @Override
    public BackendConnection openConnection() {
        return new BackendConnectionImpl(this, sqlInterface, r2dTranslator, identifierFactory, ridGenerator);
    }

    @Override
    public void enableDataImportMode(MetaSnapshot snapshot) throws RollbackException {
        if (sqlInterface.getDbBackend().isOnDataInsertMode()) {
            if (snapshot.streamMetaDatabases().findAny().isPresent()) {
                throw new IllegalStateException("Can not disable indexes if any database exists");
            }

            sqlInterface.getDbBackend().disableInternalIndexes();
        }
    }

    @Override
    public void disableDataImportMode(MetaSnapshot snapshot) throws RollbackException {
        if (!sqlInterface.getDbBackend().isOnDataInsertMode()) {
            sqlInterface.getDbBackend().enableInternalIndexes();

            //create indexes
            Stream<Consumer<DSLContext>> createIndexesJobs = snapshot.streamMetaDatabases().flatMap(
                    db -> db.streamMetaCollections().flatMap(
                            col -> col.streamContainedMetaDocParts().flatMap(
                                    docPart -> enableInternalIndexJobs(db, col, docPart)
                            )
                    )
            );

            //backend specific jobs
            Stream<Consumer<DSLContext>> backendSpecificJobs = sqlInterface.getStructureInterface()
                    .streamDataInsertFinishTasks(snapshot);
            Stream<Runnable> runnables = Stream.concat(createIndexesJobs, backendSpecificJobs)
                    .map(this::dslConsumerToRunnable);
            try {
                streamExecutor.executeRunnables(runnables)
                        .join();
            } catch (CompletionException ex) {
                if (ex.getCause() != null && ex.getCause() instanceof RollbackException) {
                    throw (RollbackException) ex.getCause();
                }
                throw ex;
            }
        }
    }

    private Stream<Consumer<DSLContext>> enableInternalIndexJobs(MetaDatabase db, MetaCollection col, MetaDocPart docPart) {
        StructureInterface structureInterface = sqlInterface.getStructureInterface();

        Stream<Consumer<DSLContext>> consumerStream;

        if (docPart.getTableRef().isRoot()) {
            consumerStream = structureInterface.streamRootDocPartTableIndexesCreation(
                    db.getIdentifier(),
                    docPart.getIdentifier(),
                    docPart.getTableRef()
            );
        } else {
            MetaDocPart parentDocPart = col.getMetaDocPartByTableRef(
                    docPart.getTableRef().getParent().get()
            );
            assert parentDocPart != null;
            consumerStream = structureInterface.streamDocPartTableIndexesCreation(
                    db.getIdentifier(),
                    docPart.getIdentifier(),
                    docPart.getTableRef(),
                    parentDocPart.getIdentifier()
            );
        }

        return consumerStream;
    }

    private Runnable dslConsumerToRunnable(Consumer<DSLContext> consumer) {
        return () -> {
            try {
                retrier.retry(() -> {
                    try (Connection connection = sqlInterface.getDbBackend().createWriteConnection()) {
                        DSLContext dsl = sqlInterface.getDslContextFactory()
                                .createDSLContext(connection);

                        consumer.accept(dsl);
                        return null;
                    } catch (SQLException ex) {
                        throw sqlInterface.getErrorHandler().handleException(Context.CREATE_INDEX, ex);
                    }
                });
            } catch (RetrierGiveUpException ex) {
                throw new ToroRuntimeException(ex);
            }
        };
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
