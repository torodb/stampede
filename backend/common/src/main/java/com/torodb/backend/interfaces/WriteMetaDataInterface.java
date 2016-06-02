package com.torodb.backend.interfaces;

import javax.annotation.Nonnull;

import org.jooq.DSLContext;

import com.torodb.core.TableRef;

public interface WriteMetaDataInterface {
    @Nonnull String createDatabaseTableStatement(@Nonnull String schemaName, @Nonnull String tableName);
    @Nonnull String createCollectionTableStatement(@Nonnull String schemaName, @Nonnull String tableName);
    @Nonnull String createContainerTableStatement(@Nonnull String schemaName, @Nonnull String tableName);
    @Nonnull String createFieldTableStatement(@Nonnull String schemaName, @Nonnull String tableName);
    @Nonnull String createIndexesTableStatement(@Nonnull String schemaName, @Nonnull String tableName, @Nonnull String indexNameColumn, @Nonnull String indexOptionsColumn);
    
    int reserveRids(@Nonnull DSLContext dsl, @Nonnull String database, @Nonnull String collection, @Nonnull TableRef tableRef, int count);
    
    @Nonnull String createIndexStatement(@Nonnull String fullIndexName, @Nonnull String tableSchema, 
            @Nonnull String tableName, @Nonnull String tableColumnName, boolean isAscending);
    @Nonnull String dropIndexStatement(@Nonnull String schemaName, @Nonnull String indexName);
}
