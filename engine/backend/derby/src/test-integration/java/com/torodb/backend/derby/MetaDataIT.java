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

import com.torodb.backend.DslContextFactory;
import com.torodb.backend.SqlInterface;
import com.torodb.backend.meta.SchemaUpdater;
import com.torodb.backend.tables.MetaCollectionTable;
import com.torodb.backend.tables.MetaDatabaseTable;
import com.torodb.backend.tables.records.MetaCollectionRecord;
import com.torodb.backend.tables.records.MetaDatabaseRecord;
import com.torodb.core.transaction.metainf.ImmutableMetaCollection;
import com.torodb.core.transaction.metainf.ImmutableMetaDatabase;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDatabase;
import org.jooq.DSLContext;
import org.jooq.Result;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;

import static org.junit.Assert.assertEquals;

public class MetaDataIT {

    private SqlInterface sqlInterface;

    private DslContextFactory dslContextFactory;

    @Before
    public void setUp() throws Exception {
        DatabaseTestContext dbTestContext = new DatabaseTestContext();

        sqlInterface = dbTestContext.getSqlInterface();
        dslContextFactory = dbTestContext.getDslContextFactory();

        SchemaUpdater schemaUpdater = dbTestContext.getSchemaUpdater();
        try (Connection connection = sqlInterface.getDbBackend().createWriteConnection()) {
            schemaUpdater.checkOrCreate(dslContextFactory.createDslContext(connection));
            connection.commit();
        }
    }

    @Test
    public void metadataDatabaseTableCanBeWritten() throws Exception {
        try (Connection connection = sqlInterface.getDbBackend().createWriteConnection()) {
            DSLContext dslContext = dslContextFactory.createDslContext(connection);

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
            assertEquals("database_identifier", records.get(0).getIdentifier());

            connection.commit();
        }
    }

    @Test
    public void metadataCollectionTableCanBeWritten() throws Exception {
        try (Connection connection = sqlInterface.getDbBackend().createWriteConnection()) {
            DSLContext dslContext = dslContextFactory.createDslContext(connection);

            MetaDatabase metaDatabase = new ImmutableMetaDatabase.Builder("database_name", "database_identifier").build();
            MetaCollection metaCollection = new ImmutableMetaCollection.Builder("collection_name", "collection_identifier").build();
            sqlInterface.getMetaDataWriteInterface().addMetaCollection(dslContext, metaDatabase, metaCollection);

            MetaCollectionTable<MetaCollectionRecord> metaCollectionTable = sqlInterface
                    .getMetaDataReadInterface().getMetaCollectionTable();
            Result<MetaCollectionRecord> records =
                    dslContext.selectFrom(metaCollectionTable)
                            .where(metaCollectionTable.IDENTIFIER.eq("collection_identifier"))
                            .fetch();

            assertEquals(1, records.size());
            assertEquals("collection_name", records.get(0).getName());
            assertEquals("collection_identifier", records.get(0).getIdentifier());
            assertEquals("database_name", records.get(0).getDatabase());

            connection.commit();
        }
    }


}
