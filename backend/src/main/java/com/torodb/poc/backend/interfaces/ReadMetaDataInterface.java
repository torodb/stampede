package com.torodb.poc.backend.interfaces;

import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import org.jooq.DSLContext;

import com.torodb.poc.backend.mocks.Path;
import com.torodb.poc.backend.sql.index.NamedDbIndex;
import com.torodb.poc.backend.tables.CollectionTable;
import com.torodb.poc.backend.tables.ContainerTable;
import com.torodb.poc.backend.tables.DatabaseTable;
import com.torodb.poc.backend.tables.FieldTable;
import com.torodb.poc.backend.tables.records.CollectionRecord;
import com.torodb.poc.backend.tables.records.ContainerRecord;
import com.torodb.poc.backend.tables.records.DatabaseRecord;
import com.torodb.poc.backend.tables.records.FieldRecord;

public interface ReadMetaDataInterface {
    @Nonnull DatabaseTable<?> getDatabaseTable();
    @Nonnull CollectionTable<?> getCollectionTable();
    @Nonnull ContainerTable<?> getContainerTable();
    @Nonnull FieldTable<?> getFieldTable();
    
    @Nonnull Iterable<DatabaseRecord> getDatabases(@Nonnull DSLContext dsl);
    @Nonnull Iterable<CollectionRecord> getCollections(@Nonnull DSLContext dsl, @Nonnull String database);
    @Nonnull Map<String, ContainerRecord> getContainersByTable(@Nonnull DSLContext dsl, @Nonnull String database, @Nonnull String collection);
    @Nonnull Map<String, FieldRecord> getFieldsByColumn(@Nonnull DSLContext dsl, @Nonnull String database, @Nonnull String collection, @Nonnull Path path);
    
    long getDatabaseSize(@Nonnull DSLContext dsl, @Nonnull String databaseName);
    Long getCollectionSize(@Nonnull DSLContext dsl, @Nonnull String schema, @Nonnull String collection);
    Long getDocumentsSize(@Nonnull DSLContext dsl, @Nonnull String schema, @Nonnull String collection);
    Long getIndexSize(@Nonnull DSLContext dsl, @Nonnull String schema, @Nonnull String collection, @Nonnull String index, 
            @Nonnull Set<NamedDbIndex> relatedDbIndexes, @Nonnull Map<String, Integer> relatedToroIndexes);
}
