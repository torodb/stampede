/*
 * ToroDB
 * Copyright © 2014 8Kdata Technology (www.8kdata.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.torodb.backend.derby;

import com.torodb.backend.*;
import com.torodb.backend.derby.schema.DerbySchemaUpdater;
import com.torodb.backend.driver.derby.DerbyDbBackendConfiguration;
import com.torodb.backend.driver.derby.DerbyDriverProvider;
import com.torodb.backend.driver.derby.OfficialDerbyDriver;
import com.torodb.backend.tables.MetaDatabaseTable;
import com.torodb.backend.tables.records.MetaDatabaseRecord;
import com.torodb.core.backend.IdentifierConstraints;
import com.torodb.core.transaction.metainf.ImmutableMetaDatabase;
import com.torodb.core.transaction.metainf.MetaDatabase;
import org.jooq.DSLContext;
import org.jooq.Result;
import org.junit.Test;

import java.sql.Connection;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import static org.junit.Assert.assertEquals;

public class MetaDataIT {

    @Test
    public void metadataDatabaseTableCanBeWritten() throws Exception {
        DerbyDriverProvider driver = new OfficialDerbyDriver();
        ThreadFactory threadFactory = Executors.defaultThreadFactory();
        DerbyErrorHandler errorHandler = new DerbyErrorHandler();

        IdentifierConstraints constraints = new DerbyIdentifierConstraints();

        DerbyDataTypeProvider provider = new DerbyDataTypeProvider();
        SqlHelper sqlHelper = new SqlHelper(provider, errorHandler);
        DerbyDbBackendConfiguration configuration = new LocalTestDerbyDbBackendConfiguration();
        DbBackendService dbBackendService = new DerbyDbBackend(threadFactory, configuration, driver, errorHandler);
        dbBackendService.startAsync();
        dbBackendService.awaitRunning();
        DslContextFactory dslContextFactory = new DslContextFactoryImpl(provider);

        DerbyDbBackend dbBackend = new DerbyDbBackend(threadFactory, configuration, driver, errorHandler);

        DerbyMetaDataReadInterface metaDataReadInterface = new DerbyMetaDataReadInterface(sqlHelper);
        DerbyStructureInterface derbyStructureInterface = new DerbyStructureInterface(dbBackend, metaDataReadInterface, sqlHelper, constraints);

        DerbyMetaDataWriteInterface metadataWriteInterface = new DerbyMetaDataWriteInterface(metaDataReadInterface, sqlHelper);

        SqlInterface sqlInterface = new SqlInterfaceDelegate(metaDataReadInterface, metadataWriteInterface, provider, derbyStructureInterface, null, null, null, errorHandler, dslContextFactory, dbBackendService);
        DerbySchemaUpdater schemaUpdater = new DerbySchemaUpdater(sqlInterface, sqlHelper);

        try (Connection connection = sqlInterface.getDbBackend().createWriteConnection()) {
            schemaUpdater.checkOrCreate(dslContextFactory.createDslContext(connection));

            DSLContext dslContext = dslContextFactory.createDslContext(connection);;
            MetaDatabase metaDatabase = new ImmutableMetaDatabase.Builder("database_name", "database_identifier").build();
            sqlInterface.getMetaDataWriteInterface().addMetaDatabase(dslContext, metaDatabase);

            MetaDatabaseTable<MetaDatabaseRecord> metaDatabaseTable = sqlInterface
                    .getMetaDataReadInterface().getMetaDatabaseTable();
            Result<MetaDatabaseRecord> records =
                    dslContext.selectFrom(metaDatabaseTable)
                            .where(metaDatabaseTable.IDENTIFIER.eq("database_identifier"))
                            .fetch();

            assertEquals(1, records.size());
            assertEquals("database_name", records.get(0).getName());

            connection.commit();
        }
    }

}
