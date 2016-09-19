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

package com.torodb.backend.meta;

import com.google.common.io.CharStreams;
import com.torodb.backend.ErrorHandler.Context;
import com.torodb.backend.SqlHelper;
import com.torodb.backend.SqlInterface;
import com.torodb.backend.exceptions.InvalidDatabaseException;
import com.torodb.backend.tables.SemanticTable;
import com.torodb.core.exceptions.SystemException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.sql.SQLException;
import javax.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.Meta;
import org.jooq.Schema;
import org.jooq.Table;

@Singleton
public abstract class AbstractSchemaUpdater implements SchemaUpdater {

    private static final Logger LOGGER = LogManager.getLogger(AbstractSchemaUpdater.class);

    @Override
    public void checkOrCreate(
            DSLContext dsl, 
            Meta jooqMeta, 
            SqlInterface sqlInterface, 
            SqlHelper sqlHelper
    ) throws SQLException, IOException, InvalidDatabaseException {
        Schema torodbSchema = null;
        for (Schema schema : jooqMeta.getSchemas()) {
            if (sqlInterface.getIdentifierConstraints().isSameIdentifier(TorodbSchema.IDENTIFIER, schema.getName())) {
                torodbSchema = schema;
                break;
            }
        }
        if (torodbSchema == null) {
            LOGGER.info("Schema '{}' not found. Creating it...", TorodbSchema.IDENTIFIER);
            createSchema(dsl, sqlInterface, sqlHelper);
            LOGGER.info("Schema '{}' created", TorodbSchema.IDENTIFIER);
        }
        else {
            LOGGER.info("Schema '{}' found. Checking it...", TorodbSchema.IDENTIFIER);
            checkSchema(torodbSchema, sqlInterface);
            LOGGER.info("Schema '{}' checked", TorodbSchema.IDENTIFIER);
        }
    }

    protected void createSchema(DSLContext dsl, SqlInterface sqlInterface, SqlHelper sqlHelper) throws SQLException, IOException {
        sqlInterface.getStructureInterface().createSchema(dsl, TorodbSchema.IDENTIFIER);
        sqlInterface.getMetaDataWriteInterface().createMetaDatabaseTable(dsl);
        sqlInterface.getMetaDataWriteInterface().createMetaCollectionTable(dsl);
        sqlInterface.getMetaDataWriteInterface().createMetaDocPartTable(dsl);
        sqlInterface.getMetaDataWriteInterface().createMetaFieldTable(dsl);
        sqlInterface.getMetaDataWriteInterface().createMetaScalarTable(dsl);
        sqlInterface.getMetaDataWriteInterface().createMetaIndexTable(dsl);
        sqlInterface.getMetaDataWriteInterface().createMetaIndexFieldTable(dsl);
        sqlInterface.getMetaDataWriteInterface().createMetaDocPartIndexTable(dsl);
        sqlInterface.getMetaDataWriteInterface().createMetaFieldIndexTable(dsl);
    }
    
    @SuppressWarnings({ "rawtypes", "unchecked" })
    private void checkSchema(Schema torodbSchema, SqlInterface sqlInterface) throws InvalidDatabaseException {
        SemanticTable<?>[] metaTables = new SemanticTable[] {
            sqlInterface.getMetaDataReadInterface().getMetaDatabaseTable(),
            sqlInterface.getMetaDataReadInterface().getMetaCollectionTable(),
            sqlInterface.getMetaDataReadInterface().getMetaDocPartTable(),
            sqlInterface.getMetaDataReadInterface().getMetaFieldTable(),
            sqlInterface.getMetaDataReadInterface().getMetaScalarTable(),
            sqlInterface.getMetaDataReadInterface().getMetaIndexTable(),
            sqlInterface.getMetaDataReadInterface().getMetaIndexFieldTable(),
            sqlInterface.getMetaDataReadInterface().getMetaDocPartIndexTable(),
            sqlInterface.getMetaDataReadInterface().getMetaDocPartIndexColumnTable()
        };
        for (SemanticTable metaTable : metaTables) {
            String metaTableName = metaTable.getName();
            boolean metaTableFound = false;
            for (Table<?> table : torodbSchema.getTables()) {
                if (sqlInterface.getIdentifierConstraints().isSameIdentifier(table.getName(), metaTableName)) {
                    metaTable.checkSemanticallyEquals(table);
                    metaTableFound = true;
                    LOGGER.info(table + " found and check");
                }
            }
            if (!metaTableFound) {
                throw new InvalidDatabaseException("The schema '" + TorodbSchema.IDENTIFIER + "'"
                        + " does not contain the expected meta table '" 
                        + metaTableName +"'");
            }
        }
    }

    @SuppressFBWarnings(value = "UI_INHERITANCE_UNSAFE_GETRESOURCE",
            justification = "We want to read resources from the subclass")
    protected void executeSql(
            DSLContext dsl, 
            String resourcePath,
            SqlHelper sqlHelper
    ) throws IOException, SQLException {
        InputStream resourceAsStream
                = getClass().getResourceAsStream(resourcePath);
        if (resourceAsStream == null) {
            throw new SystemException(
                    "Resource '" + resourcePath + "' does not exist"
            );
        }
        try {
            String statementAsString
                    = CharStreams.toString(
                            new BufferedReader(
                                    new InputStreamReader(
                                            resourceAsStream,
                                            Charset.forName("UTF-8"))));
            sqlHelper.executeStatement(dsl, statementAsString, Context.UNKNOWN);
        } finally {
            resourceAsStream.close();
        }
    }
}
