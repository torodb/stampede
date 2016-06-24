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

package com.torodb.backend.postgresql;

import java.util.Collection;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.jooq.Field;

import com.torodb.backend.AbstractStructureInterface;
import com.torodb.backend.SqlBuilder;
import com.torodb.backend.SqlHelper;

/**
 *
 */
@Singleton
public class PostgreSQLStructureInterface extends AbstractStructureInterface {

    @Inject
    public PostgreSQLStructureInterface(SqlHelper sqlHelper) {
        super(sqlHelper);
    }

    @Override
    protected String getDropSchemaStatement(String schemaName) {
        return "DROP SCHEMA \"" + schemaName + "\" CASCADE";
    }
    
    @Override
    protected String getCreateIndexStatement(String indexName, String schemaName, String tableName, String columnName,
            boolean ascending) {
        StringBuilder sb = new StringBuilder()
                .append("CREATE INDEX ")
                .append("\"").append(indexName).append("\"")
                .append(" ON ")
                .append("\"").append(schemaName).append("\"")
                .append(".")
                .append("\"").append(tableName).append("\"")
                .append(" (")
                    .append("\"").append(columnName).append("\"")
                    .append(" ").append(ascending ? "ASC" : "DESC")
                .append(")");
        String statement = sb.toString();
        return statement;
    }

    @Override
    protected String getDropIndexStatement(String schemaName, String indexName) {
        StringBuilder sb = new StringBuilder()
                .append("DROP INDEX \"")
                .append(schemaName)
                .append("\".\"")
                .append(indexName)
                .append("\" CASCADE");
        String statement = sb.toString();
        return statement;
    }

    @Override
    protected String getCreateSchemaStatement(String schemaName) {
        return "CREATE SCHEMA IF NOT EXISTS \"" + schemaName + "\"";
    }

    @Override
    protected String getCreateDocPartTableStatement(String schemaName, String tableName,
            Collection<? extends Field<?>> fields) {
    	SqlBuilder sb = new SqlBuilder("CREATE TABLE ");
    	sb.table(schemaName, tableName)
		  .append(" (");
		int cont = 0;
		for (Field<?> field : getFieldIterator(fields)) {
			if (cont > 0) {
				sb.append(',');
			}
			sb.quote(field.getName()).append(' ').append(field.getDataType().getCastTypeName());
			cont++;
		}
		sb.append(')');
		String statement = sb.toString();
        return statement;
    }
	
    @Override
    protected String getAddColumnToDocPartTableStatement(String schemaName, String tableName,
            Field<?> field) {
    	SqlBuilder sb = new SqlBuilder("ALTER TABLE ")
    		.table(schemaName, tableName)
            .append(" ADD COLUMN ")
            .quote(field.getName())
            .append(" ")
            .append(field.getDataType().getCastTypeName());
        return sb.toString();
    }
}
