package com.torodb.torod.db.backends.meta;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jooq.ConnectionProvider;
import org.jooq.DSLContext;
import org.jooq.Name;
import org.jooq.Record1;
import org.jooq.Record2;
import org.jooq.Result;
import org.jooq.TableField;
import org.jooq.impl.DSL;
import org.jooq.impl.TableImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;
import com.torodb.torod.core.exceptions.ToroRuntimeException;
import com.torodb.torod.core.pojos.NamedToroIndex;
import com.torodb.torod.db.backends.DatabaseInterface;
import com.torodb.torod.db.backends.converters.json.ToroIndexToJsonConverter;
import com.torodb.torod.db.backends.sql.index.NamedDbIndex;
import com.torodb.torod.db.backends.sql.index.UnnamedDbIndex;
import com.torodb.torod.db.backends.tables.SubDocTable;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public abstract class AbstractIndexStorage implements IndexStorage {
    private static final long serialVersionUID = 1L;

    private final DatabaseInterface databaseInterface;
    
    private AbstractToroIndexTable toroIndexTable;
    private ToroIndexToJsonConverter indexToJsonConverter;
    private CollectionSchema colSchema;
    
    private static final Logger LOGGER
            = LoggerFactory.getLogger(AbstractIndexStorage.class);
    
    public AbstractIndexStorage(String databaseName, CollectionSchema colSchema, DatabaseInterface databaseInterface) {
        this.indexToJsonConverter = new ToroIndexToJsonConverter(
                databaseName,
                colSchema.getCollection()
        );
        this.toroIndexTable = createIndexTable(colSchema, indexToJsonConverter);
        this.colSchema = colSchema;
        this.databaseInterface = databaseInterface;
    }
    
    protected abstract AbstractToroIndexTable createIndexTable(CollectionSchema colSchema, ToroIndexToJsonConverter indexToJsonConverter);
    
    @Override
    public void initialize(DSLContext dsl, String databaseName, CollectionSchema colSchema) {
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

            dsl.execute(
                    databaseInterface.createIndexesTableStatement(
                            dsl.render(tableName), toroIndexTable.nameColumn.getName(),
                            toroIndexTable.indexColumn.getName()
                    )
            );
        }
    }
    
    @Override
    public Set<NamedDbIndex> getAllDbIndexes(DSLContext dsl) {
        Connection connection
                = dsl.configuration().connectionProvider().acquire();

        try {
            Set<NamedDbIndex> result = Sets.newHashSet();

            String schema = colSchema.getName();
            DatabaseMetaData metaData = connection.getMetaData();
            String lastIndexName = null;

            for (SubDocTable table : colSchema.getSubDocTables()) {
                try (ResultSet indexInfo = databaseInterface.getIndexes(metaData, schema, table.getName())) {
                    while (indexInfo.next()) {
                        String indexName = indexInfo.getString("INDEX_NAME");

                        if (!isDbIndexName(indexName)) {
                            LOGGER.trace("{} is not recognized as a db index name", indexInfo);
                            continue;
                        }
                        String columnName = indexInfo.getString("COLUMN_NAME");
                        boolean ascending
                                = indexInfo.getString("ASC_OR_DESC") == null ||
                                indexInfo.getString("ASC_OR_DESC").equals("A");

                        if (lastIndexName != null
                                && lastIndexName.equals(indexName)) {
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
            }

            return result;
        } catch (SQLException ex) {
            throw new ToroRuntimeException(ex);
        } finally {
            dsl.configuration().connectionProvider().release(connection);
        }
    }

    @Override
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
    @Override
    public void dropIndex(DSLContext dsl, NamedDbIndex index) {
        ConnectionProvider connectionProvider
                = dsl.configuration().connectionProvider();
        Connection connection = connectionProvider.acquire();
        try (Statement st = connection.createStatement()) {
            String query = databaseInterface.dropIndexStatement(colSchema.getName(), index.getName());
            st.executeUpdate(query);
        } catch (SQLException ex) {
            throw new ToroRuntimeException(ex);
        } finally {
            connectionProvider.release(connection);
        }
    }

    private boolean isDbIndexName(String indexName) {
        Pattern p = Pattern.compile("t_\\d+_\\S+");
        Matcher matcher = p.matcher(indexName);
        return matcher.find();
    }

    @SuppressFBWarnings("SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE")
    @Override
    public NamedDbIndex createIndex(DSLContext dsl, UnnamedDbIndex unnamedDbIndex) {
        String indexName = unnamedDbIndex.getTable() + '_' + unnamedDbIndex.getColumn();
        indexName = colSchema.getDatabaseInterface().escapeIndexName(indexName);
        
        ConnectionProvider connectionProvider
                = dsl.configuration().connectionProvider();
        Connection connection = connectionProvider.acquire();
        try (Statement st = connection.createStatement()) {
            String query = databaseInterface.createIndexStatement(
                    indexName, unnamedDbIndex.getSchema(), unnamedDbIndex.getTable(), unnamedDbIndex.getColumn(),
                    unnamedDbIndex.isAscending()
            );
            LOGGER.debug("Creating a db index with query: " + query);
            st.executeUpdate(query);
        } catch (SQLException ex) {
            throw new ToroRuntimeException(ex);
        } finally {
            connectionProvider.release(connection);
        }
        
        return new NamedDbIndex(indexName, unnamedDbIndex);
    }
    
    @Override
    public void eventToroIndexRemoved(DSLContext dsl, String indexName) {
        dsl.delete(toroIndexTable).where(toroIndexTable.nameColumn.eq(indexName)).execute();
    }

    @Override
    public void eventToroIndexCreated(DSLContext dsl, NamedToroIndex index) {
        dsl.insertInto(toroIndexTable)
                .set(toroIndexTable.nameColumn, index.getName())
                .set(toroIndexTable.indexColumn, index)
                .execute();
    }

    protected static abstract class AbstractToroIndexTable extends TableImpl<Record2<String, NamedToroIndex>> {
        private static final long serialVersionUID = 1L;
        final TableField<Record2<String, NamedToroIndex>, String> nameColumn;
        final TableField<Record2<String, NamedToroIndex>, NamedToroIndex> indexColumn;

        public AbstractToroIndexTable(CollectionSchema colSchema, ToroIndexToJsonConverter indexToJsonConverter) {
            super("indexes", colSchema);
            nameColumn = createNameField();
            indexColumn = createIndexField(indexToJsonConverter);
        }

        protected abstract TableField<Record2<String, NamedToroIndex>, String> createNameField();
        
        protected abstract TableField<Record2<String, NamedToroIndex>, NamedToroIndex> createIndexField(ToroIndexToJsonConverter indexToJsonConverter);

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
            final AbstractToroIndexTable other = (AbstractToroIndexTable) obj;
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
