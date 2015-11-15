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
package com.torodb.torod.db.wrappers.postgresql.meta;


import com.torodb.torod.db.wrappers.exceptions.InvalidDatabaseException;
import com.torodb.torod.db.wrappers.postgresql.meta.tables.CollectionsTable;
import com.torodb.torod.db.wrappers.sql.AutoCloser;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.jooq.DSLContext;
import org.jooq.Meta;
import org.jooq.Schema;

import org.jooq.Table;
import org.jooq.impl.SchemaImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TorodbSchema extends SchemaImpl {

    private static final Logger LOGGER = LoggerFactory.getLogger(TorodbSchema.class);
	private static final long serialVersionUID = -1813122131;

	/**
	 * The reference instance of <code>torodb</code>
	 */
	public static final TorodbSchema TORODB = new TorodbSchema();

	/**
	 * No further instances allowed
	 */
	private TorodbSchema() {
		super("torodb");
	}

    void checkOrCreate(
            DSLContext dsl, 
            Meta jooqMeta, 
            DatabaseMetaData jdbcMeta) throws SQLException, InvalidDatabaseException {
        Schema torodbSchema = null;
        for (Schema schema : jooqMeta.getSchemas()) {
            if (schema.getName().equals("torodb")) {
                torodbSchema = schema;
                break;
            }
        }
        if (torodbSchema == null) {
            LOGGER.info("Schema 'torodb' not found. Creating it...");
            createSchema(dsl);
            LOGGER.info("Schema 'torodb' created");
        }
        else {
            LOGGER.info("Schema 'torodb' found. Checking it...");
            checkSchema(torodbSchema);
            LOGGER.info("Schema 'torodb' checked");
        }
    }

	@Override
	public final List<Table<?>> getTables() {
		List result = new ArrayList();
		result.addAll(getTables0());
		return result;
	}

	private final List<Table<?>> getTables0() {
		return Arrays.<Table<?>>asList(CollectionsTable.COLLECTIONS);
	}

    @SuppressFBWarnings("SQL_PREPARED_STATEMENT_GENERATED_FROM_NONCONSTANT_STRING")
    private void createSchema(DSLContext dsl) throws SQLException {
        Connection c = dsl.configuration().connectionProvider().acquire();

        PreparedStatement ps = null;
        try {
            ps = c.prepareStatement("CREATE SCHEMA \"torodb\"");
            ps.executeUpdate();
            AutoCloser.close(ps);
            
            ps = c.prepareStatement(CollectionsTable.COLLECTIONS.getSQLCreationStatement(c));
            ps.execute();
        } finally {
            AutoCloser.close(ps);
            dsl.configuration().connectionProvider().release(c);
        }
    }

    private void checkSchema(Schema torodbSchema) throws InvalidDatabaseException {
        CollectionsTable colsTable = CollectionsTable.COLLECTIONS;
        String colsTableName = colsTable.getName();
        boolean collectionsTableFound = false;
        for (Table<?> table : torodbSchema.getTables()) {
            if (table.getName().equals(colsTableName)) {
                if (!colsTable.isSemanticallyEquals(table)) {
                    throw new InvalidDatabaseException("It was expected that "
                            + "the table "+table+" was the collections table, "
                            + "but they are not semantically equals");
                }
                collectionsTableFound = true;
                LOGGER.info(table + " found and check");
            }
        }
        if (!collectionsTableFound) {
            throw new InvalidDatabaseException("The schema torodb " 
                    + getName() + " does not contain the expected table '" 
                    + colsTableName +"'");
        }
    }
}
