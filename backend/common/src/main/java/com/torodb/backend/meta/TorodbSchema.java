/*
 *     This file is part of ToroDB.
 *
 *     ToroDB is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Affero General PublicSchema License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     ToroDB is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Affero General PublicSchema License for more details.
 *
 *     You should have received a copy of the GNU Affero General PublicSchema License
 *     along with ToroDB. If not, see <http://www.gnu.org/licenses/>.
 *
 *     Copyright (c) 2014, 8Kdata Technology
 *     
 */
package com.torodb.backend.meta;


import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.Meta;
import org.jooq.Schema;
import org.jooq.Table;
import org.jooq.impl.SchemaImpl;

import com.torodb.backend.DatabaseInterface;
import com.torodb.backend.exceptions.InvalidDatabaseException;
import com.torodb.backend.tables.SemanticTable;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class TorodbSchema extends SchemaImpl {

    private static final Logger LOGGER = LogManager.getLogger(TorodbSchema.class);
	private static final long serialVersionUID = -1813122131;

    public static final String TORODB_SCHEMA = "torodb";

    /**
     * The reference instance of <code>torodb</code>
     */
    public static final TorodbSchema TORODB = new TorodbSchema();

    /**
     * No further instances allowed
     */
	protected TorodbSchema() {
		super(TORODB_SCHEMA);
	}

    public void checkOrCreate(
            DSLContext dsl, 
            Meta jooqMeta, 
            DatabaseInterface databaseInterface
    ) throws SQLException, InvalidDatabaseException {
        Schema torodbSchema = null;
        for (Schema schema : jooqMeta.getSchemas()) {
            if (databaseInterface.isSameIdentifier(TORODB_SCHEMA, schema.getName())) {
                torodbSchema = schema;
                break;
            }
        }
        if (torodbSchema == null) {
            LOGGER.info("Schema '{}' not found. Creating it...", TORODB_SCHEMA);
            createSchema(dsl, databaseInterface);
            LOGGER.info("Schema '{}' created", TORODB_SCHEMA);
        }
        else {
            LOGGER.info("Schema '{}' found. Checking it...", TORODB_SCHEMA);
            checkSchema(torodbSchema, databaseInterface);
            LOGGER.info("Schema '{}' checked", TORODB_SCHEMA);
        }
    }

	@Override
	public final List<Table<?>> getTables() {
	    throw new RuntimeException("operation not permitted");
	}

    @SuppressFBWarnings("SQL_PREPARED_STATEMENT_GENERATED_FROM_NONCONSTANT_STRING")
    private void createSchema(DSLContext dsl, DatabaseInterface databaseInterface) throws SQLException {
        Connection c = dsl.configuration().connectionProvider().acquire();
        
        try (PreparedStatement ps = c.prepareStatement(databaseInterface.createSchemaStatement(TORODB_SCHEMA))) {
            ps.executeUpdate();
        }

        try (PreparedStatement ps = c.prepareStatement(databaseInterface.getMetaDatabaseTable().getSQLCreationStatement(databaseInterface))) {
            ps.execute();
        } finally {
            dsl.configuration().connectionProvider().release(c);
        }

        try (PreparedStatement ps = c.prepareStatement(databaseInterface.getMetaCollectionTable().getSQLCreationStatement(databaseInterface))) {
            ps.execute();
        } finally {
            dsl.configuration().connectionProvider().release(c);
        }

        try (PreparedStatement ps = c.prepareStatement(databaseInterface.getMetaDocPartTable().getSQLCreationStatement(databaseInterface))) {
            ps.execute();
        } finally {
            dsl.configuration().connectionProvider().release(c);
        }

        try (PreparedStatement ps = c.prepareStatement(databaseInterface.getMetaFieldTable().getSQLCreationStatement(databaseInterface))) {
            ps.execute();
        } finally {
            dsl.configuration().connectionProvider().release(c);
        }
    }
    
    private void checkSchema(Schema torodbSchema, DatabaseInterface databaseInterface) throws InvalidDatabaseException {
        SemanticTable<?>[] metaTables = new SemanticTable[] {
            databaseInterface.getMetaDatabaseTable(),
            databaseInterface.getMetaCollectionTable(),
            databaseInterface.getMetaDocPartTable(),
            databaseInterface.getMetaFieldTable()
        };
        for (SemanticTable metaTable : metaTables) {
            String metaTableName = metaTable.getName();
            boolean metaTableFound = false;
            for (Table<?> table : torodbSchema.getTables()) {
                if (databaseInterface.isSameIdentifier(table.getName(), metaTableName)) {
                    metaTable.checkSemanticallyEquals(table);
                    metaTableFound = true;
                    LOGGER.info(table + " found and check");
                }
            }
            if (!metaTableFound) {
                throw new InvalidDatabaseException("The schema '" + getName() + "'"
                        + " does not contain the expected meta table '" 
                        + metaTableName +"'");
            }
        }
    }
}
