package com.torodb.backend;

import com.torodb.backend.converters.jooq.DataTypeForKV;
import com.torodb.core.TableRef;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MetaSnapshot;
import java.util.function.Consumer;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import org.jooq.DSLContext;

public interface StructureInterface {
    void createSchema(@Nonnull DSLContext dsl, @Nonnull String schemaName);
    void dropDatabase(@Nonnull DSLContext dsl, @Nonnull MetaDatabase metaDatabase);
    void dropCollection(@Nonnull DSLContext dsl, @Nonnull String schemaName, @Nonnull MetaCollection metaCollection);
    void renameCollection(@Nonnull DSLContext dsl, @Nonnull String fromSchemaName, @Nonnull MetaCollection fromCollection, 
            @Nonnull String toSchemaName, @Nonnull MetaCollection toCollection);
    void createRootDocPartTable(@Nonnull DSLContext dsl, @Nonnull String schemaName, @Nonnull String tableName, @Nonnull TableRef tableRef);
    void createDocPartTable(@Nonnull DSLContext dsl, @Nonnull String schemaName, @Nonnull String tableName, @Nonnull TableRef tableRef,
            @Nonnull String foreignTableName);

    /**
     * Returns a stream of consumers that, when executed, creates the required indexes on a root
     * doc part table.
     *
     * The returned stream is empty if the backend is not including the internal indexes
     * @param schemaName
     * @param tableName
     * @param tableRef
     * @return
     * @see DbBackend#includeInternalIndexes()
     */
    Stream<Consumer<DSLContext>> streamRootDocPartTableIndexesCreation(String schemaName, String tableName, TableRef tableRef);
    
    /**
     * Returns a stream of consumers that, when executed, creates the required indexes on a doc part
     * table.
     *
     * The returned stream is empty if the backend is not including the internal indexes
     * @param schemaName
     * @param tableName
     * @param tableRef
     * @param foreignTableName
     * @return
     * @see DbBackend#includeInternalIndexes()
     */
    Stream<Consumer<DSLContext>> streamDocPartTableIndexesCreation(String schemaName, String tableName, TableRef tableRef, String foreignTableName);
    void addColumnToDocPartTable(@Nonnull DSLContext dsl, @Nonnull String schemaName, @Nonnull String tableName, @Nonnull String columnName, @Nonnull DataTypeForKV<?> dataType);

    /**
     * Returns a stream of consumers that, when executed, executes backend specific tasks that
     * should be done once the data insert mode finishes.
     *
     * For example, PostgreSQL backend would like to run analyze on the modified tables to get some
     * stadistics.
     * @param snapshot
     * @return
     */
    public Stream<Consumer<DSLContext>> streamDataInsertFinishTasks(MetaSnapshot snapshot);
    
    void createIndex(@Nonnull DSLContext dsl, @Nonnull String tableSchema, 
            @Nonnull String tableName, @Nonnull String tableColumnName, boolean isAscending);
    void dropIndex(@Nonnull DSLContext dsl, @Nonnull String schemaName, @Nonnull String indexName);
}
