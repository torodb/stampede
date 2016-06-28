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

package com.torodb.integration.backend;

import java.sql.Connection;
import java.util.Arrays;

import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.impl.DSL;
import org.junit.Assume;
import org.junit.Test;

import com.torodb.core.transaction.RollbackException;
import com.torodb.core.transaction.metainf.FieldType;
import com.torodb.integration.Backend;
import com.torodb.integration.IntegrationTestEnvironment;

public class BackendRollbackIntegrationTest extends AbstractBackendTest {
    @Test(expected=RollbackException.class)
    public void testCreateSameSchema() throws Exception {
        Assume.assumeFalse("POstgreSQL use CREATE SCHEMA IF NOT EXISTS so does not fire any exception", 
                IntegrationTestEnvironment.CURRENT_INTEGRATION_TEST_ENVIRONMENT.getBackend() == Backend.POSTGRES);
        
        try (
             Connection leftConnection = sqlInterface.createWriteConnection();
             Connection rightConnection = sqlInterface.createWriteConnection()
            ) {
            DSLContext leftDsl = sqlInterface.createDSLContext(leftConnection);
            DSLContext rightDsl = sqlInterface.createDSLContext(rightConnection);
            sqlInterface.createSchema(leftDsl, "test");
            leftConnection.commit();
            sqlInterface.createSchema(rightDsl, "test");
            rightConnection.commit();
        }
    }
    
    @Test(expected=RollbackException.class)
    public void testCreateSameTable() throws Exception {
        try (Connection connection = sqlInterface.createWriteConnection()) {
            DSLContext dsl = sqlInterface.createDSLContext(connection);
            sqlInterface.createSchema(dsl, "test");
            connection.commit();
        }
        
        try (
             Connection leftConnection = sqlInterface.createWriteConnection();
             Connection rightConnection = sqlInterface.createWriteConnection()
            ) {
            DSLContext leftDsl = sqlInterface.createDSLContext(leftConnection);
            DSLContext rightDsl = sqlInterface.createDSLContext(rightConnection);
            sqlInterface.createDocPartTable(leftDsl, "test", "test", Arrays.asList(new Field<?>[] { DSL.field("dummy", sqlInterface.getDataType(FieldType.STRING)) }));
            leftConnection.commit();
            sqlInterface.createDocPartTable(rightDsl, "test", "test", Arrays.asList(new Field<?>[] { DSL.field("dummy", sqlInterface.getDataType(FieldType.STRING)) }));
            rightConnection.commit();
        }
    }
    
    @Test(expected=RollbackException.class)
    public void testCreateSameColumn() throws Exception {
        try (Connection connection = sqlInterface.createWriteConnection()) {
            DSLContext dsl = sqlInterface.createDSLContext(connection);
            sqlInterface.createSchema(dsl, "test");
            sqlInterface.createDocPartTable(dsl, "test", "test", Arrays.asList(new Field<?>[] { DSL.field("dummy", sqlInterface.getDataType(FieldType.STRING)) }));
            connection.commit();
        }
        
        try (
             Connection leftConnection = sqlInterface.createWriteConnection();
             Connection rightConnection = sqlInterface.createWriteConnection()
            ) {
            DSLContext leftDsl = sqlInterface.createDSLContext(leftConnection);
            DSLContext rightDsl = sqlInterface.createDSLContext(rightConnection);
            sqlInterface.addColumnToDocPartTable(leftDsl, "test", "test", DSL.field("test", sqlInterface.getDataType(FieldType.STRING)));
            leftConnection.commit();
            sqlInterface.addColumnToDocPartTable(rightDsl, "test", "test", DSL.field("test", sqlInterface.getDataType(FieldType.STRING)));
            rightConnection.commit();
        }
    }
}
