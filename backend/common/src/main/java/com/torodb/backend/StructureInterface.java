package com.torodb.backend;

import javax.annotation.Nonnull;

import org.jooq.DSLContext;

import com.torodb.backend.converters.jooq.DataTypeForKV;
import com.torodb.core.TableRef;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDatabase;

public interface StructureInterface {
    void createSchema(@Nonnull DSLContext dsl, @Nonnull String schemaName);
    void dropDatabase(@Nonnull DSLContext dsl, @Nonnull MetaDatabase metaDatabase);
    void dropCollection(@Nonnull DSLContext dsl, @Nonnull String schemaName, @Nonnull MetaCollection metaCollection);
    void renameCollection(@Nonnull DSLContext dsl, @Nonnull String fromSchemaName, @Nonnull MetaCollection fromCollection, 
            @Nonnull String toSchemaName, @Nonnull MetaCollection toCollection);
    void createRootDocPartTable(@Nonnull DSLContext dsl, @Nonnull String schemaName, @Nonnull String tableName, @Nonnull TableRef tableRef);
    void createDocPartTable(@Nonnull DSLContext dsl, @Nonnull String schemaName, @Nonnull String tableName, @Nonnull TableRef tableRef,
            @Nonnull String foreignTableName);
    void addRootDocPartTableIndexes(@Nonnull DSLContext dsl, @Nonnull String schemaName, @Nonnull String tableName, @Nonnull TableRef tableRef);
    void addDocPartTableIndexes(@Nonnull DSLContext dsl, @Nonnull String schemaName, @Nonnull String tableName, @Nonnull TableRef tableRef, @Nonnull String foreignTableName);
    void addColumnToDocPartTable(@Nonnull DSLContext dsl, @Nonnull String schemaName, @Nonnull String tableName, @Nonnull String columnName, @Nonnull DataTypeForKV<?> dataType);
    
    void createIndex(@Nonnull DSLContext dsl, @Nonnull String tableSchema, 
            @Nonnull String tableName, @Nonnull String tableColumnName, boolean isAscending);
    void dropIndex(@Nonnull DSLContext dsl, @Nonnull String schemaName, @Nonnull String indexName);
}
