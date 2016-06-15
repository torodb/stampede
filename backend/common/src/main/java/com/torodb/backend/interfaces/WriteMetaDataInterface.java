package com.torodb.backend.interfaces;

import javax.annotation.Nonnull;

import org.jooq.DSLContext;

import com.torodb.core.TableRef;

public interface WriteMetaDataInterface {
    @Nonnull String createMetaDatabaseTableStatement(@Nonnull String schemaName, @Nonnull String tableName);
    @Nonnull String createMetaCollectionTableStatement(@Nonnull String schemaName, @Nonnull String tableName);
    @Nonnull String createMetaDocPartTableStatement(@Nonnull String schemaName, @Nonnull String tableName);
    @Nonnull String createMetaFieldTableStatement(@Nonnull String schemaName, @Nonnull String tableName);
    @Nonnull String createMetaIndexesTableStatement(@Nonnull String schemaName, @Nonnull String tableName, @Nonnull String indexNameColumn, @Nonnull String indexOptionsColumn);
    
    void addMetaDatabase(@Nonnull DSLContext dsl, @Nonnull String databaseName, @Nonnull String databaseIdentifier);
    void addMetaCollection(@Nonnull DSLContext dsl, @Nonnull String databaseName, @Nonnull String collectionName, @Nonnull String collectionIdentifier);
    void addMetaDocPart(@Nonnull DSLContext dsl, @Nonnull String databaseName, @Nonnull String collectionName, @Nonnull TableRef tableRef, @Nonnull String docPartIdentifier);
    
    int consumeRids(@Nonnull DSLContext dsl, @Nonnull String database, @Nonnull String collection, @Nonnull TableRef tableRef, int count);
    
    @Nonnull String createIndexStatement(@Nonnull String fullIndexName, @Nonnull String tableSchema, 
            @Nonnull String tableName, @Nonnull String tableColumnName, boolean isAscending);
    @Nonnull String dropIndexStatement(@Nonnull String schemaName, @Nonnull String indexName);
}
