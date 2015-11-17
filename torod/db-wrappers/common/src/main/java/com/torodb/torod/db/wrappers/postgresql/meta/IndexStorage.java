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

import com.google.common.collect.Sets;
import com.torodb.torod.core.exceptions.ToroRuntimeException;
import com.torodb.torod.core.pojos.NamedToroIndex;
import com.torodb.torod.db.wrappers.converters.json.ToroIndexToJsonConverter;
import com.torodb.torod.db.wrappers.postgresql.meta.tables.SubDocTable;
import com.torodb.torod.db.wrappers.sql.AutoCloser;
import com.torodb.torod.db.wrappers.sql.index.NamedDbIndex;
import com.torodb.torod.db.wrappers.sql.index.UnnamedDbIndex;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.jooq.*;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.jooq.impl.TableImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.*;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 */
public class IndexStorage implements Serializable {
    private static final long serialVersionUID = 1L;

    private final ToroIndexTable toroIndexTable;
    private final ToroIndexToJsonConverter indexToJsonConverter;
    private final CollectionSchema colSchema;
    
    private static final Logger LOGGER
            = LoggerFactory.getLogger(IndexStorage.class);
    
    public IndexStorage(String databaseName, CollectionSchema colSchema) {
        this.indexToJsonConverter = new ToroIndexToJsonConverter(
                databaseName,
                colSchema.getCollection()
        );
        this.toroIndexTable = new ToroIndexTable(colSchema, indexToJsonConverter);
        this.colSchema = colSchema;
    }
    
    public void initialize(DSLContext dsl) {
        createIndexTableIfNotExists(dsl);
    }

    private void createIndexTableIfNotExists(DSLContext dsl) {
        boolean tableExists = dsl.select(DSL.count())
                .from("information_schema.tables")
                .where(
                        DSL.field("table_schema").eq(colSchema.getName())
                        .and(DSL.field("table_name").eq(toroIndexTable.getName()))
                )
                .fetchOne(0, int.class) > 0;

        if (!tableExists) {
            Name tableName = DSL.name(colSchema.getName(), toroIndexTable.getName());
            dsl.execute("CREATE TABLE " + dsl.render(tableName) + " ("
                    + toroIndexTable.nameColumn.getName() + " VARCHAR PRIMARY KEY, "
                    + toroIndexTable.indexColumn.getName() + " JSONB NOT NULL"
                    + ")");
        }
    }
    
    public Set<NamedDbIndex> getAllDbIndexes(DSLContext dsl) {
        Connection connection
                = dsl.configuration().connectionProvider().acquire();
        ResultSet indexInfo = null;

        try {
            Set<NamedDbIndex> result = Sets.newHashSet();

            String catalog = connection.getCatalog();
            String schema = colSchema.getName();
            DatabaseMetaData metaData = connection.getMetaData();
            String lastIndexName = null;

            for (SubDocTable table : colSchema.getSubDocTables()) {
                indexInfo = metaData.getIndexInfo(
                        catalog,
                        schema,
                        table.getName(),
                        false,
                        false
                );
                while (indexInfo.next()) {
                    String indexName = indexInfo.getString("INDEX_NAME");
                    
                    if (!isDbIndexName(indexName)) {
                        LOGGER.trace("{} is not recognized as a db index name", indexInfo);
                        continue ;
                    }
                    String columnName = indexInfo.getString("COLUMN_NAME");
                    boolean ascending
                            = indexInfo.getString("ASC_OR_DESC").equals("A");

                    if (lastIndexName != null && lastIndexName.equals(indexName)) {
                        LOGGER.warn("Index {} is recognized as a multiple column "
                                + "index, which are not supported", lastIndexName);
                        continue;
                    }
                    lastIndexName = indexName;

                    result.add(
                            new NamedDbIndex(
                                    indexName,
                                    new UnnamedDbIndex(
                                            schema,
                                            table.getName(),
                                            columnName,
                                            ascending
                                    )
                            )
                    );
                }

                indexInfo.close();
            }

            return result;
        }
        catch (SQLException ex) {
            throw new ToroRuntimeException(ex);
        }
        finally {
            AutoCloser.close(indexInfo);
            dsl.configuration().connectionProvider().release(connection);
        }
    }

    public Set<NamedToroIndex> getAllToroIndexes(DSLContext dsl) {
        Result<Record1<NamedToroIndex>> storedIndexes = dsl
                .select(toroIndexTable.indexColumn)
                .from(toroIndexTable)
                .fetch();

        Set<NamedToroIndex> result = Sets.newHashSet();
        for (Record1<NamedToroIndex> storedIndex : storedIndexes) {
            result.add(storedIndex.value1());
        }
        return result;
    }

    @SuppressFBWarnings("SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE")
    public void dropIndex(DSLContext dsl, NamedDbIndex index) {
        ConnectionProvider connectionProvider
                = dsl.configuration().connectionProvider();
        Connection connection = connectionProvider.acquire();
        Statement st = null;
        try {
            String query = "DROP INDEX \"" + colSchema.getName() + "\".\"" + index.getName() + "\"";
            st = connection.createStatement();
            st.executeUpdate(query);
        } catch (SQLException ex) {
            throw new ToroRuntimeException(ex);
        } finally {
            AutoCloser.close(st);
            connectionProvider.release(connection);
        }
    }

    private boolean isDbIndexName(String indexName) {
        Pattern p = Pattern.compile("t_\\d+_\\S+");
        Matcher matcher = p.matcher(indexName);
        return matcher.find();
    }

    @SuppressFBWarnings("SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE")
    public NamedDbIndex createIndex(DSLContext dsl, UnnamedDbIndex unnamedDbIndex) {
        String indexName = unnamedDbIndex.getTable() + '_' + unnamedDbIndex.getColumn();
        indexName = colSchema.getDatabaseInterface().escapeIndexName(indexName);
        
        ConnectionProvider connectionProvider
                = dsl.configuration().connectionProvider();
        Connection connection = connectionProvider.acquire();
        Statement st = null;
        try {
            st = connection.createStatement();
            String query = "CREATE INDEX \"" + indexName + "\" ON "
                    + "\"" + unnamedDbIndex.getSchema() + "\".\""+ unnamedDbIndex.getTable() + "\" ("
                    + "\"" + unnamedDbIndex.getColumn() + "\" "
                    + (unnamedDbIndex.isAscending() ? "ASC" : "DESC") + ')';
            LOGGER.debug("Creating a db index with query: " + query);
            st.executeUpdate(query);
        } catch (SQLException ex) {
            throw new ToroRuntimeException(ex);
        } finally {
            AutoCloser.close(st);
            connectionProvider.release(connection);
        }
        
        return new NamedDbIndex(indexName, unnamedDbIndex);
    }
    
    public void eventToroIndexRemoved(DSLContext dsl, String indexName) {
        dsl.delete(toroIndexTable).where(toroIndexTable.nameColumn.eq(indexName)).execute();
    }

    public void eventToroIndexCreated(DSLContext dsl, NamedToroIndex index) {
        dsl.insertInto(toroIndexTable)
                .set(toroIndexTable.nameColumn, index.getName())
                .set(toroIndexTable.indexColumn, index)
                .execute();
    }

    private static class ToroIndexTable extends TableImpl<Record2<String, NamedToroIndex>> {

        private static final long serialVersionUID = 1L;
        final TableField<Record2<String, NamedToroIndex>, String> nameColumn;
        final TableField<Record2<String, NamedToroIndex>, NamedToroIndex> indexColumn;

        public ToroIndexTable(CollectionSchema colSchema, ToroIndexToJsonConverter indexToJsonConverter) {
            super("indexes", colSchema);
            nameColumn = createField("name", SQLDataType.VARCHAR, this);
            indexColumn = createField("index", SQLDataType.VARCHAR, this, "", indexToJsonConverter);
        }

        @Override
        public int hashCode() {
            int hash = 7;
            hash
                    = 53 * hash +
                    (this.nameColumn != null ? this.nameColumn.hashCode() : 0);
            hash
                    = 53 * hash +
                    (this.indexColumn != null ? this.indexColumn.hashCode() : 0);
            return hash;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            final ToroIndexTable other = (ToroIndexTable) obj;
            if (this.nameColumn != other.nameColumn &&
                    (this.nameColumn == null ||
                    !this.nameColumn.equals(other.nameColumn))) {
                return false;
            }
            if (this.indexColumn != other.indexColumn &&
                    (this.indexColumn == null ||
                    !this.indexColumn.equals(other.indexColumn))) {
                return false;
            }
            return true;
        }

    }
    
}
