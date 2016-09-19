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

import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Singleton;

import org.jooq.DSLContext;

import com.google.common.base.Preconditions;
import com.torodb.backend.AbstractStructureInterface;
import com.torodb.backend.ErrorHandler.Context;
import com.torodb.backend.InternalField;
import com.torodb.backend.SqlBuilder;
import com.torodb.backend.SqlHelper;
import com.torodb.backend.converters.jooq.DataTypeForKV;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.core.transaction.metainf.MetaSnapshot;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 *
 */
@Singleton
public class PostgreSQLStructureInterface extends AbstractStructureInterface {

    private SqlHelper sqlHelper;
    
    @Inject
    public PostgreSQLStructureInterface(PostgreSQLDbBackend dbBackend, PostgreSQLMetaDataReadInterface metaDataReadInterface, SqlHelper sqlHelper) {
        super(dbBackend, metaDataReadInterface, sqlHelper);
        
        this.sqlHelper = sqlHelper;
    }

    @Override
    public void dropDatabase(@Nonnull DSLContext dsl, @Nonnull MetaDatabase metaDatabase) {
        String statement = getDropSchemaStatement(metaDatabase.getIdentifier());
        sqlHelper.executeUpdate(dsl, statement, Context.DROP_SCHEMA);
    }

    @Override
    protected String getDropTableStatement(String schemaName, String tableName) {
        return "DROP TABLE \"" + schemaName + "\".\"" + tableName + "\"";
    }
    
    @Override
    protected String getRenameTableStatement(String fromSchemaName, String fromTableName, 
            String toTableName) {
        return "ALTER TABLE \"" + fromSchemaName + "\".\"" + fromTableName + "\" RENAME TO \"" + toTableName + "\"";
    }

    @Override
    protected String getSetTableSchemaStatement(String fromSchemaName, String fromTableName, 
            String toSchemaName) {
        return "ALTER TABLE \"" + fromSchemaName + "\".\"" + fromTableName + "\" SET SCHEMA = \"" + toSchemaName + "\"";
    }

    @Override
    protected String getDropSchemaStatement(String schemaName) {
        return "DROP SCHEMA \"" + schemaName + "\" CASCADE";
    }
    
    @Override
    protected String getCreateIndexStatement(String indexName, String schemaName, String tableName, String columnName,
            boolean ascending, boolean unique) {
        String uniqueText = unique ? "UNIQUE " : "";

        StringBuilder sb = new StringBuilder()
                .append("CREATE ").append(uniqueText).append("INDEX ")
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
            Collection<InternalField<?>> fields) {
        SqlBuilder sb = new SqlBuilder("CREATE TABLE ");
        sb.table(schemaName, tableName)
          .append(" (");
        if (!fields.isEmpty()) {
            for (InternalField<?> field : fields) {
                sb.quote(field.getName()).append(' ')
                    .append(field.getDataType().getCastTypeName());
                if (!field.isNullable()) {
                    sb.append(" NOT NULL");
                }
                sb.append(',');
            }
            sb.setLastChar(')');
        } else {
            sb.append(')');
        }
        return sb.toString();
    }

    @Override
    protected String getAddDocPartTablePrimaryKeyStatement(String schemaName, String tableName,
            Collection<InternalField<?>> primaryKeyFields) {
        SqlBuilder sb = new SqlBuilder("ALTER TABLE ");
        sb.table(schemaName, tableName)
          .append(" ADD PRIMARY KEY (");
        for (InternalField<?> field : primaryKeyFields) {
            sb.quote(field.getName()).append(',');
        }
        sb.setLastChar(')');
        return sb.toString();
    }

    @Override
    protected String getAddDocPartTableForeignKeyStatement(String schemaName, String tableName,
            Collection<InternalField<?>> referenceFields, String foreignTableName,
            Collection<InternalField<?>> foreignFields) {
        Preconditions.checkArgument(referenceFields.size() == foreignFields.size());
        
        SqlBuilder sb = new SqlBuilder("ALTER TABLE ");
        sb.table(schemaName, tableName)
          .append(" ADD FOREIGN KEY (");
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
        sb.setLastChar(')');
        return sb.toString();
    }

    @Override
    protected String getCreateDocPartTableIndexStatement(String schemaName, String tableName,
            Collection<InternalField<?>> indexFields) {
        Preconditions.checkArgument(!indexFields.isEmpty());
        SqlBuilder sb = new SqlBuilder("CREATE INDEX ON ");
        sb.table(schemaName, tableName)
          .append(" (");
        for (InternalField<?> field : indexFields) {
            sb.quote(field.getName()).append(',');
        }
        sb.setLastChar(')');
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

    @Override
    public Stream<Consumer<DSLContext>> streamDataInsertFinishTasks(MetaSnapshot snapshot) {
        return snapshot.streamMetaDatabases().flatMap(
                db -> db.streamMetaCollections().flatMap(
                        col -> col.streamContainedMetaDocParts().map(
                                docPart -> createAnalyzeConsumer(db, col, docPart)
                        )
                )
        );
    }

    private Consumer<DSLContext> createAnalyzeConsumer(MetaDatabase db, MetaCollection col, MetaDocPart docPart) {
        return dsl -> {
            SqlBuilder sb = new SqlBuilder("ANALYZE ")
                    .table(db.getIdentifier(), docPart.getIdentifier());
            dsl.execute(sb.toString());
        };
    }
}
