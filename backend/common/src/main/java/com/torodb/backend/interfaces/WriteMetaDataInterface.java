package com.torodb.backend.interfaces;

import java.sql.Connection;

import javax.annotation.Nonnull;

import org.jooq.DSLContext;

import com.torodb.core.TableRef;
import com.torodb.core.transaction.metainf.FieldType;

public interface WriteMetaDataInterface {
    @Nonnull Connection createSystemConnection();

    @Nonnull String createMetaIndexesTableStatement(@Nonnull String schemaName, @Nonnull String tableName, @Nonnull String indexNameColumn, @Nonnull String indexOptionsColumn);
    
    void createMetaDatabaseTable(@Nonnull DSLContext dsl);
    void createMetaCollectionTable(@Nonnull DSLContext dsl);
    void createMetaDocPartTable(@Nonnull DSLContext dsl);
    void createMetaFieldTable(@Nonnull DSLContext dsl);
    void createMetaScalarTable(@Nonnull DSLContext dsl);
    
    void addMetaDatabase(@Nonnull DSLContext dsl, @Nonnull String databaseName, @Nonnull String databaseIdentifier);
    void addMetaCollection(@Nonnull DSLContext dsl, @Nonnull String databaseName, @Nonnull String collectionName, @Nonnull String collectionIdentifier);
    void addMetaDocPart(@Nonnull DSLContext dsl, @Nonnull String databaseName, @Nonnull String collectionName, @Nonnull TableRef tableRef, @Nonnull String docPartIdentifier);
    void addMetaField(@Nonnull DSLContext dsl, @Nonnull String databaseName, @Nonnull String collectionName, @Nonnull TableRef tableRef, @Nonnull String fieldName, String fieldIdentifier, FieldType type);
    void addMetaScalar(@Nonnull DSLContext dsl, @Nonnull String databaseName, @Nonnull String collectionName, @Nonnull TableRef tableRef, String fieldIdentifier, FieldType type);
    
    int consumeRids(@Nonnull DSLContext dsl, @Nonnull String database, @Nonnull String collection, @Nonnull TableRef tableRef, int count);
}
