package com.torodb.backend.derby;

import com.torodb.backend.*;
import com.torodb.backend.derby.schema.DerbySchemaUpdater;
import com.torodb.backend.driver.derby.DerbyDbBackendConfiguration;
import com.torodb.backend.driver.derby.DerbyDriverProvider;
import com.torodb.backend.driver.derby.OfficialDerbyDriver;
import com.torodb.backend.meta.SchemaUpdater;
import com.torodb.core.backend.IdentifierConstraints;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

public class DatabaseTestContext {

    private SqlInterface sqlInterface;

    private DslContextFactory dslContextFactory;

    private SchemaUpdater schemaUpdater;

    public DatabaseTestContext() {
        DerbyErrorHandler errorHandler = new DerbyErrorHandler();
        DataTypeProvider provider = new DerbyDataTypeProvider();
        SqlHelper sqlHelper = new SqlHelper(provider, errorHandler);

        sqlInterface = buildSqlInterface(provider, sqlHelper, errorHandler);
        dslContextFactory = buildDslContextFactory(provider);
        schemaUpdater = buildSchemaUpdater(sqlInterface, sqlHelper);
    }

    private SqlInterface buildSqlInterface(DataTypeProvider provider, SqlHelper sqlHelper, DerbyErrorHandler errorHandler) {
        DerbyDriverProvider driver = new OfficialDerbyDriver();
        ThreadFactory threadFactory = Executors.defaultThreadFactory();

        IdentifierConstraints constraints = new DerbyIdentifierConstraints();


        DerbyDbBackendConfiguration configuration = new LocalTestDerbyDbBackendConfiguration();

        DerbyDbBackend dbBackend = new DerbyDbBackend(threadFactory, configuration, driver, errorHandler);

        DerbyMetaDataReadInterface metaDataReadInterface = new DerbyMetaDataReadInterface(sqlHelper);
        DerbyStructureInterface derbyStructureInterface = new DerbyStructureInterface(dbBackend, metaDataReadInterface, sqlHelper, constraints);

        DerbyMetaDataWriteInterface metadataWriteInterface = new DerbyMetaDataWriteInterface(metaDataReadInterface, sqlHelper);

        DbBackendService dbBackendService = new DerbyDbBackend(threadFactory, configuration, driver, errorHandler);
        dbBackendService.startAsync();
        dbBackendService.awaitRunning();

        return new SqlInterfaceDelegate(metaDataReadInterface, metadataWriteInterface, provider, derbyStructureInterface,
                null, null, null, errorHandler, dslContextFactory, dbBackendService);
    }

    private DslContextFactory buildDslContextFactory(DataTypeProvider provider) {
        return new DslContextFactoryImpl(provider);
    }

    private SchemaUpdater buildSchemaUpdater(SqlInterface sqlInterface, SqlHelper sqlHelper) {
        return new DerbySchemaUpdater(sqlInterface, sqlHelper);
    }

    public SchemaUpdater getSchemaUpdater() {
        return schemaUpdater;
    }

    public SqlInterface getSqlInterface() {
        return sqlInterface;
    }

    public DslContextFactory getDslContextFactory() {
        return dslContextFactory;
    }

}
