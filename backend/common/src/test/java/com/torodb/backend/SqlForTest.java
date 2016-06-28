package com.torodb.backend;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Singleton;
import com.torodb.backend.meta.SchemaUpdater;
import com.torodb.backend.meta.SnapshotUpdater;
import com.torodb.core.TableRefFactory;
import com.torodb.core.guice.CoreModule;
import com.torodb.core.transaction.metainf.ImmutableMetaSnapshot;
import com.torodb.core.transaction.metainf.MetainfoRepository.SnapshotStage;
import com.torodb.metainfo.cache.mvcc.MvccMetainfoRepository;

public class SqlForTest {

    private SqlInterface sqlInterface;
    private SchemaUpdater schemaUpdater;
    private SqlHelper sqlHelper;
    
    public SqlForTest(DatabaseForTest database) throws SQLException{
        Injector injector = createInjector(database);
        DbBackendService dbBackend = injector.getInstance(DbBackendService.class);
        dbBackend.startAsync();
        dbBackend.awaitRunning();
        database.cleanDatabase(injector);
        sqlInterface = injector.getInstance(SqlInterface.class);
        sqlHelper = injector.getInstance(SqlHelper.class);
        schemaUpdater = injector.getInstance(SchemaUpdater.class);
    }
    
    private Injector createInjector(DatabaseForTest database){
        Module sqlModule = new Module() {
            @Override
            public void configure(Binder binder) {
                binder.bind(SqlInterface.class)
                    .to(SqlInterfaceDelegate.class)
                    .in(Singleton.class);
                binder.bind(DslContextFactory.class)
                    .to(DslContextFactoryImpl.class)
                    .asEagerSingleton();
            }
        };
    	
    	List<Module> modules = new ArrayList<>();
    	modules.add(new CoreModule());
    	modules.add(sqlModule);
    	modules.addAll(database.getModules());
        return Guice.createInjector(modules.toArray(new Module[]{}));
    }
    

    public SqlInterface getSqlInterface(){
    	return sqlInterface;
    }
    
    public ImmutableMetaSnapshot buildMetaSnapshot(TableRefFactory tableRefFactory) {
        MvccMetainfoRepository metainfoRepository = new MvccMetainfoRepository();
        SnapshotUpdater.updateSnapshot(metainfoRepository, sqlInterface, sqlHelper, schemaUpdater, tableRefFactory);

        try (SnapshotStage stage = metainfoRepository.startSnapshotStage()) {
            return stage.createImmutableSnapshot();
        }
    }

}
