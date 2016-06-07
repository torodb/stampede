/*
 *     This file is part of ToroDB.
 *
 *     ToroDB is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Affero General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     ToroDB is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Affero General Public License for more details.
 *
 *     You should have received a copy of the GNU Affero General Public License
 *     along with ToroDB. If not, see <http://www.gnu.org/licenses/>.
 *
 *     Copyright (c) 2014, 8Kdata Technology
 *     
 */

package com.torodb.backend.derby;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.List;

import javax.sql.DataSource;

import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.torodb.backend.converters.jooq.DataTypeForKV;
import com.torodb.backend.driver.derby.DerbyDbBackendConfiguration;
import com.torodb.backend.driver.derby.OfficialDerbyDriver;
import com.torodb.backend.meta.TorodbSchema;
import com.torodb.core.TableRef;
import com.torodb.core.impl.TableRefImpl;
import com.torodb.core.transaction.metainf.FieldType;

public class BackendDerbyTest {
    
    private DerbyDatabaseInterface databaseInterface;
    private DataSource dataSource;
    
    @Before
    public void setUp() throws Exception {
        OfficialDerbyDriver derbyDriver = new OfficialDerbyDriver();
        dataSource = derbyDriver.getConfiguredDataSource(new DerbyDbBackendConfiguration() {
            @Override
            public String getUsername() {
                return null;
            }
            
            @Override
            public int getReservedReadPoolSize() {
                return 0;
            }
            
            @Override
            public String getPassword() {
                return null;
            }
            
            @Override
            public int getDbPort() {
                return 0;
            }
            
            @Override
            public String getDbName() {
                return "torodb";
            }
            
            @Override
            public String getDbHost() {
                return null;
            }
            
            @Override
            public long getCursorTimeout() {
                return 0;
            }
            
            @Override
            public long getConnectionPoolTimeout() {
                return 0;
            }
            
            @Override
            public int getConnectionPoolSize() {
                return 0;
            }

            @Override
            public boolean inMemory() {
                return false;
            }

            @Override
            public boolean embedded() {
                return true;
            }
        }, "torod");
        databaseInterface = new DerbyDatabaseInterface();
        try (Connection connection = dataSource.getConnection()) {
            DatabaseMetaData metaData = connection.getMetaData();
            ResultSet tables = metaData.getTables("%", "%", "%", null);
            while (tables.next()) {
                String schemaName = tables.getString("TABLE_SCHEM");
                String tableName = tables.getString("TABLE_NAME");
                if (!databaseInterface.isRestrictedSchemaName(schemaName) || schemaName.equals(TorodbSchema.TORODB_SCHEMA)) {
                    try (PreparedStatement preparedStatement = connection.prepareStatement("DROP TABLE \"" + schemaName + "\".\"" + tableName + "\"")) {
                        preparedStatement.executeUpdate();
                    }
                }
            }
            ResultSet schemas = metaData.getSchemas();
            while (schemas.next()) {
                String schemaName = schemas.getString("TABLE_SCHEM");
                if (!databaseInterface.isRestrictedSchemaName(schemaName) || schemaName.equals(TorodbSchema.TORODB_SCHEMA)) {
                    try (PreparedStatement preparedStatement = connection.prepareStatement("DROP SCHEMA \"" + schemaName + "\" RESTRICT")) {
                        preparedStatement.executeUpdate();
                    }
                }
            }
            connection.commit();
        }
    }
    
    @Test
    public void testTorodbMeta() throws Exception {
        try (Connection connection = dataSource.getConnection()) {
            new DerbyTorodbMeta(DSL.using(connection, SQLDialect.DERBY), databaseInterface);
            connection.commit();
        }
        try (Connection connection = dataSource.getConnection()) {
            new DerbyTorodbMeta(DSL.using(connection, SQLDialect.DERBY), databaseInterface);
            connection.commit();
        }
    }
    
    @Test
    public void testTorodbMetaStoreAndReload() throws Exception {
        String databaseName = "databaseName";
        String databaseSchema = "database_schema";
        String collectionName = "collectionName";
        TableRef rootDocPartTableRef = TableRefImpl.createRoot();
        String rootDocPartTableName = "rootDocPartTableName";
        List<Field<?>> rootDocPartFields = Arrays.asList(new Field[] {
                DSL.field("nullRootField", databaseInterface.getDataType(FieldType.NULL)),
                DSL.field("booleanRootField", databaseInterface.getDataType(FieldType.BOOLEAN)),
                DSL.field("integerRootField", databaseInterface.getDataType(FieldType.INTEGER)),
                DSL.field("longRootField", databaseInterface.getDataType(FieldType.LONG)),
                DSL.field("doubleRootField", databaseInterface.getDataType(FieldType.DOUBLE)),
                DSL.field("stringRootField", databaseInterface.getDataType(FieldType.STRING)),
                DSL.field("dateRootField", databaseInterface.getDataType(FieldType.DATE)),
                DSL.field("timeRootField", databaseInterface.getDataType(FieldType.TIME)),
                DSL.field("mongoObjectIdRootField", databaseInterface.getDataType(FieldType.MONGO_OBJECT_ID)),
                DSL.field("mongoTimeStampRootField", databaseInterface.getDataType(FieldType.MONGO_TIME_STAMP)),
                DSL.field("instantRootField", databaseInterface.getDataType(FieldType.INSTANT)),
                DSL.field("subDocPart", databaseInterface.getDataType(FieldType.CHILD)),
        });
        TableRef subDocPartTableRef = TableRefImpl.createChild(rootDocPartTableRef, "subDocPart");
        String subDocPartTableName = "subDocPartTableName";
        List<Field<?>> subDocPartFields = Arrays.asList(new Field[] {
                DSL.field("nullSubField", databaseInterface.getDataType(FieldType.NULL)),
                DSL.field("booleanSubField", databaseInterface.getDataType(FieldType.BOOLEAN)),
                DSL.field("integerSubField", databaseInterface.getDataType(FieldType.INTEGER)),
                DSL.field("longSubField", databaseInterface.getDataType(FieldType.LONG)),
                DSL.field("doubleSubField", databaseInterface.getDataType(FieldType.DOUBLE)),
                DSL.field("stringSubField", databaseInterface.getDataType(FieldType.STRING)),
                DSL.field("dateSubField", databaseInterface.getDataType(FieldType.DATE)),
                DSL.field("timeSubField", databaseInterface.getDataType(FieldType.TIME)),
                DSL.field("mongoObjectIdSubField", databaseInterface.getDataType(FieldType.MONGO_OBJECT_ID)),
                DSL.field("mongoTimeStampSubField", databaseInterface.getDataType(FieldType.MONGO_TIME_STAMP)),
                DSL.field("instantSubField", databaseInterface.getDataType(FieldType.INSTANT)),
        });
        
        try (Connection connection = dataSource.getConnection()) {
            new DerbyTorodbMeta(DSL.using(connection, SQLDialect.DERBY), databaseInterface);
            connection.commit();
        }
        try (Connection connection = dataSource.getConnection()) {
            try {
                DSLContext dsl = DSL.using(connection, SQLDialect.DERBY);
                dsl.insertInto(databaseInterface.getMetaDatabaseTable())
                    .set(databaseInterface.getMetaDatabaseTable().newRecord().values(databaseName, databaseSchema))
                    .execute();
                dsl.execute(databaseInterface.createSchemaStatement(databaseSchema));
                dsl.insertInto(databaseInterface.getMetaCollectionTable())
                    .set(databaseInterface.getMetaCollectionTable().newRecord().values(databaseName, collectionName))
                    .execute();
                dsl.insertInto(databaseInterface.getMetaDocPartTable())
                    .set(databaseInterface.getMetaDocPartTable().newRecord().values(databaseName, collectionName, rootDocPartTableRef, rootDocPartTableName))
                    .execute();
                for (Field field : rootDocPartFields) {
                    dsl.insertInto(databaseInterface.getMetaFieldTable())
                        .set(databaseInterface.getMetaFieldTable().newRecord().values(databaseName, collectionName, rootDocPartTableRef, 
                                field.getName(), field.getName(), 
                                FieldType.from(((DataTypeForKV<?>) field.getDataType()).getKVValueConverter().getErasuredType())))
                        .execute();
                }
                dsl.execute(databaseInterface.createDocPartTableStatement(dsl.configuration(), databaseSchema, rootDocPartTableName, rootDocPartFields));
                dsl.insertInto(databaseInterface.getMetaDocPartTable())
                    .set(databaseInterface.getMetaDocPartTable().newRecord().values(databaseName, collectionName, subDocPartTableRef, subDocPartTableName))
                    .execute();
                for (Field field : subDocPartFields) {
                    dsl.insertInto(databaseInterface.getMetaFieldTable())
                        .set(databaseInterface.getMetaFieldTable().newRecord().values(databaseName, collectionName, subDocPartTableRef, 
                                field.getName(), field.getName(), 
                                FieldType.from(((DataTypeForKV<?>) field.getDataType()).getKVValueConverter().getErasuredType())))
                        .execute();
                }
                dsl.execute(databaseInterface.createDocPartTableStatement(dsl.configuration(), databaseSchema, subDocPartTableName, subDocPartFields));
                connection.commit();
            } catch(Exception exception) {
                connection.rollback();
                
                throw exception;
            }
        }
        DerbyTorodbMeta derbyTorodbMeta;
        try (Connection connection = dataSource.getConnection()) {
            derbyTorodbMeta = new DerbyTorodbMeta(DSL.using(connection, SQLDialect.DERBY), databaseInterface);
            connection.commit();
        }
        Assert.assertNotNull(derbyTorodbMeta.getCurrentMetaSnapshot().getMetaDatabaseByName(databaseName));
        Assert.assertEquals(derbyTorodbMeta.getCurrentMetaSnapshot().getMetaDatabaseByName(databaseName).getIdentifier(), databaseSchema);
        Assert.assertNotNull(derbyTorodbMeta.getCurrentMetaSnapshot().getMetaDatabaseByName(databaseName).getMetaCollectionByName(collectionName));
        Assert.assertEquals(derbyTorodbMeta.getCurrentMetaSnapshot().getMetaDatabaseByName(databaseName)
                .getMetaCollectionByName(collectionName).getIdentifier(), rootDocPartTableName);
        Assert.assertNotNull(derbyTorodbMeta.getCurrentMetaSnapshot().getMetaDatabaseByName(databaseName).getMetaCollectionByName(collectionName)
                .getMetaDocPartByTableRef(rootDocPartTableRef));
        Assert.assertEquals(derbyTorodbMeta.getCurrentMetaSnapshot().getMetaDatabaseByName(databaseName).getMetaCollectionByName(collectionName)
                .getMetaDocPartByTableRef(rootDocPartTableRef).getIdentifier(), rootDocPartTableName);
        Assert.assertNotNull(derbyTorodbMeta.getCurrentMetaSnapshot().getMetaDatabaseByName(databaseName).getMetaCollectionByName(collectionName)
                .getMetaDocPartByTableRef(subDocPartTableRef));
        Assert.assertEquals(derbyTorodbMeta.getCurrentMetaSnapshot().getMetaDatabaseByName(databaseName).getMetaCollectionByName(collectionName)
                .getMetaDocPartByTableRef(subDocPartTableRef).getIdentifier(), subDocPartTableName);
    }
}
