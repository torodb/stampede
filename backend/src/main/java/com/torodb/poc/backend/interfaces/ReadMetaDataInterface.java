package com.torodb.poc.backend.interfaces;

import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import org.jooq.DSLContext;

import com.torodb.core.TableRef;
import com.torodb.poc.backend.sql.index.NamedDbIndex;
import com.torodb.poc.backend.tables.MetaCollectionTable;
import com.torodb.poc.backend.tables.MetaDatabaseTable;
import com.torodb.poc.backend.tables.MetaDocPartTable;
import com.torodb.poc.backend.tables.MetaFieldTable;
import com.torodb.poc.backend.tables.records.MetaCollectionRecord;
import com.torodb.poc.backend.tables.records.MetaDatabaseRecord;
import com.torodb.poc.backend.tables.records.MetaDocPartRecord;
import com.torodb.poc.backend.tables.records.MetaFieldRecord;

public interface ReadMetaDataInterface {
    @Nonnull MetaDatabaseTable<?> getMetaDatabaseTable();
    @Nonnull MetaCollectionTable<?> getMetaCollectionTable();
    @Nonnull MetaDocPartTable<?, ?> getMetaDocPartTable();
    @Nonnull MetaFieldTable<?, ?> getMetaFieldTable();
    
    @Nonnull Iterable<MetaDatabaseRecord> getDatabases(@Nonnull DSLContext dsl);
    @Nonnull Iterable<MetaCollectionRecord> getCollections(@Nonnull DSLContext dsl, @Nonnull String database);
    @Nonnull Map<String, MetaDocPartRecord<?>> getContainersByTable(@Nonnull DSLContext dsl, @Nonnull String database, @Nonnull String collection);
    @Nonnull Map<String, MetaFieldRecord<?>> getFieldsByColumn(@Nonnull DSLContext dsl, @Nonnull String database, @Nonnull String collection, @Nonnull TableRef tableRef);
    
    long getDatabaseSize(@Nonnull DSLContext dsl, @Nonnull String databaseName);
    Long getCollectionSize(@Nonnull DSLContext dsl, @Nonnull String schema, @Nonnull String collection);
    Long getDocumentsSize(@Nonnull DSLContext dsl, @Nonnull String schema, @Nonnull String collection);
    Long getIndexSize(@Nonnull DSLContext dsl, @Nonnull String schema, @Nonnull String collection, @Nonnull String index, 
            @Nonnull Set<NamedDbIndex> relatedDbIndexes, @Nonnull Map<String, Integer> relatedToroIndexes);
}
