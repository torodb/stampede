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

package com.torodb.backend;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Singleton;

import org.jooq.DSLContext;
import org.jooq.Field;

import com.google.common.collect.Lists;
import com.torodb.backend.ErrorHandler.Context;
import com.torodb.backend.tables.MetaDocPartTable.DocPartTableFields;

/**
 *
 */
@Singleton
public abstract class AbstractStructureInterface implements StructureInterface {

    private final SqlHelper sqlHelper;
    private final FieldComparator fieldComparator = new FieldComparator();
    
    @Inject
    public AbstractStructureInterface(SqlHelper sqlHelper) {
        this.sqlHelper = sqlHelper;
    }

    @Override
    public void dropSchema(@Nonnull DSLContext dsl, @Nonnull String schemaName) {
    	String statement = getDropSchemaStatement(schemaName);
    	sqlHelper.executeUpdate(dsl, statement, Context.ddl);
    }

    protected String getDropSchemaStatement(String schemaName) {
        String statement = "DROP SCHEMA \"" + schemaName + "\" CASCADE";
        return statement;
    }
    
    @Override
    public void createIndex(@Nonnull DSLContext dsl,
            @Nonnull String indexName, @Nonnull String schemaName, @Nonnull String tableName,
            @Nonnull String columnName, boolean ascending
    ) {
        String statement = getCreateIndexStatement(indexName, schemaName, tableName, columnName, ascending);

        sqlHelper.executeUpdate(dsl, statement, Context.ddl);
    }

    protected abstract String getCreateIndexStatement(String indexName, String schemaName, String tableName, String columnName,
            boolean ascending);
    
    @Override
    public void dropIndex(@Nonnull DSLContext dsl, @Nonnull String schemaName, @Nonnull String indexName) {
        String statement = getDropIndexStatement(schemaName, indexName);
        
        sqlHelper.executeUpdate(dsl, statement, Context.ddl);
    }

    protected String getDropIndexStatement(String schemaName, String indexName) {
        StringBuilder sb = new StringBuilder()
                .append("DROP INDEX ")
                .append("\"").append(schemaName).append("\"")
                .append(".")
                .append("\"").append(indexName).append("\"");
        String statement = sb.toString();
        return statement;
    }

    @Override
    public void createSchema(@Nonnull DSLContext dsl, @Nonnull String schemaName){
    	String statement = getCreateSchemaStatement(schemaName);
    	sqlHelper.executeUpdate(dsl, statement, Context.ddl);
    }

    protected abstract String getCreateSchemaStatement(String schemaName);

	@Override
	public void createDocPartTable(DSLContext dsl, String schemaName, String tableName, Collection<Field<?>> fields) {
		String statement = getCreateDocPartTableStatement(schemaName, tableName, fields);
		sqlHelper.executeStatement(dsl, statement, Context.ddl);
	}

    protected abstract String getCreateDocPartTableStatement(String schemaName, String tableName,
            Collection<Field<?>> fields);
	
    @Override
    public void addColumnToDocPartTable(DSLContext dsl, String schemaName, String tableName, Field<?> field) {
        String statement = getAddColumnToDocPartTableStatement(schemaName, tableName, field);
        
        sqlHelper.executeStatement(dsl, statement, Context.ddl);
    }

    protected abstract String getAddColumnToDocPartTableStatement(String schemaName, String tableName,
            Field<?> field);

    protected Iterable<Field<?>> getFieldIterator(Iterable<Field<?>> fields) {
        List<Field<?>> fieldList = Lists.newArrayList(fields);
        Collections.sort(fieldList, fieldComparator);
        return fieldList;
    }

    private static class FieldComparator implements Comparator<Field<?>>, Serializable {

        private static final List<Integer> sqlTypeOrder = Arrays.asList(new Integer[]{
                    java.sql.Types.NULL,
                    java.sql.Types.DOUBLE,
                    java.sql.Types.BIGINT,
                    java.sql.Types.INTEGER,
                    java.sql.Types.FLOAT,
                    java.sql.Types.TIME,
                    java.sql.Types.DATE,
                    java.sql.Types.REAL,
                    java.sql.Types.TINYINT,
                    java.sql.Types.CHAR,
                    java.sql.Types.BIT,
                    java.sql.Types.BINARY
                });
        private static final long serialVersionUID = 1L;

        @Override
        public int compare(Field<?> o1, Field<?> o2) {
            if (o1.getName().equals(DocPartTableFields.DID)) {
                return -1;
            } else if (o2.getName().equals(DocPartTableFields.DID)) {
                return 1;
            }
            if (o1.getName().equals(DocPartTableFields.RID)) {
                return -1;
            } else if (o2.getName().equals(DocPartTableFields.RID)) {
                return 1;
            }
            if (o1.getName().equals(DocPartTableFields.PID)) {
                return -1;
            } else if (o2.getName().equals(DocPartTableFields.PID)) {
                return 1;
            }
            if (o1.getName().equals(DocPartTableFields.SEQ)) {
                return -1;
            } else if (o2.getName().equals(DocPartTableFields.SEQ)) {
                return 1;
            }

            int i1 = sqlTypeOrder.indexOf(o1.getDataType().getSQLType());
            int i2 = sqlTypeOrder.indexOf(o2.getDataType().getSQLType());

            if (i1 == i2) {
                return o1.getName().compareTo(o2.getName());
            }
            if (i1 == -1) {
                return 1;
            }
            if (i2 == -1) {
                return -1;
            }
            return i1 - i2;
        }

    }
}
