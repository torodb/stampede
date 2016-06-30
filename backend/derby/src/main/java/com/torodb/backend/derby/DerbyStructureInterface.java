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

import java.util.Collection;
import java.util.Collections;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.base.Preconditions;
import com.torodb.backend.AbstractStructureInterface;
import com.torodb.backend.InternalField;
import com.torodb.backend.SqlBuilder;
import com.torodb.backend.SqlHelper;
import com.torodb.backend.converters.jooq.DataTypeForKV;

/**
 *
 */
@Singleton
public class DerbyStructureInterface extends AbstractStructureInterface {

    @Inject
    public DerbyStructureInterface(DerbyMetaDataReadInterface metaDataReadInterface, SqlHelper sqlHelper) {
        super(metaDataReadInterface, sqlHelper);
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
        return sb.toString();
    }

    @Override
    protected String getDropIndexStatement(String schemaName, String indexName) {
        StringBuilder sb = new StringBuilder()
                .append("DROP INDEX \"")
                .append(schemaName)
                .append("\".\"")
                .append(indexName)
                .append('"');
        return sb.toString();
    }

    @Override
    protected String getCreateSchemaStatement(String schemaName) {
        return "CREATE SCHEMA \"" + schemaName + "\"";
    }

    @Override
    protected String getCreateDocPartTableStatement(String schemaName, String tableName,
            Collection<InternalField<?>> fields, Collection<InternalField<?>> primaryKeyFields) {
        return getCreateDocPartTableStatement(schemaName, tableName, primaryKeyFields, primaryKeyFields, 
                Collections.emptyList(), null, Collections.emptyList());
    }

    @Override
    protected String getCreateDocPartTableStatement(String schemaName, String tableName,
            Collection<InternalField<?>> fields, Collection<InternalField<?>> primaryKeyFields,
            Collection<InternalField<?>> referenceFields, String foreignTableName,
            Collection<InternalField<?>> foreignFields) {
        Preconditions.checkArgument(referenceFields.size() == foreignFields.size());
        
        SqlBuilder sb = new SqlBuilder("CREATE TABLE ");
        sb.table(schemaName, tableName)
          .append(" (");
        if (!fields.isEmpty()) {
            for (InternalField<?> field : fields) {
                sb.quote(field.getName()).append(' ')
                    .append(field.getDataType().getCastTypeName()).append(',');
            }
            if (!primaryKeyFields.isEmpty()) {
                sb.append("PRIMARY KEY (");
                for (InternalField<?> field : primaryKeyFields) {
                    sb.quote(field.getName()).append(',');
                }
                sb.setLastChar(')').append(',');
            }
            if (!referenceFields.isEmpty()) {
                sb.append("FOREIGN KEY (");
                for (InternalField<?> field : referenceFields) {
                    sb.quote(field.getName()).append(',');
                }
                sb.setLastChar(')')
                    .append(" REFERENCES ")
                    .table(schemaName, foreignTableName)
                    .append(" (");
                for (InternalField<?> field : foreignFields) {
                    sb.quote(field.getName()).append(',');
                }
                sb.setLastChar(')').append(',');
            }
            sb.setLastChar(')');
        } else {
            sb.append(')');
        }
        return sb.toString();
    }

    @Override
    protected String getAddColumnToDocPartTableStatement(String schemaName, String tableName, String columnName,
            DataTypeForKV<?> dataType) {
        SqlBuilder sb = new SqlBuilder("ALTER TABLE ")
                .table(schemaName, tableName)
                .append(" ADD COLUMN ")
                .quote(columnName)
                .append(" ")
                .append(dataType.getCastTypeName());
            return sb.toString();
    }
}
