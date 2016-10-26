
package com.torodb.backend;

import com.google.inject.assistedinject.Assisted;
import com.torodb.core.backend.*;
import com.torodb.core.modules.AbstractBundle;
import com.torodb.core.supervision.Supervisor;
import java.util.concurrent.ThreadFactory;
import javax.inject.Inject;

/**
 *
 */
public class BackendBundleImpl extends AbstractBundle implements BackendBundle {

    private final DbBackendService lowLevelService;
    private final BackendService backendService;

    @Inject
    public BackendBundleImpl(DbBackendService lowLevelService,
            BackendServiceImpl backendService, ThreadFactory threadFactory,
            @Assisted Supervisor supervisor) {
        super(threadFactory, supervisor);
        this.lowLevelService = lowLevelService;
        this.backendService = backendService;
    }

    @Override
    protected void postDependenciesStartUp() throws Exception {
        lowLevelService.startAsync();
        lowLevelService.awaitRunning();

        backendService.startAsync();
        backendService.awaitRunning();
        
        try (BackendConnection conn = backendService.openConnection();
                ExclusiveWriteBackendTransaction trans = conn.openExclusiveWriteTransaction()) {

            trans.checkOrCreateMetaDataTables();
            trans.commit();
        }
    }

    @Override
    protected void preDependenciesShutDown() throws Exception {
        backendService.stopAsync();
        backendService.awaitTerminated();

        lowLevelService.stopAsync();
        lowLevelService.awaitTerminated();
    }

    @Override
    public BackendService getBackendService() {
        return backendService;
    }

}
