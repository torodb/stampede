package com.torodb.backend.interfaces;

import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import org.jooq.DSLContext;

import com.torodb.backend.sql.index.NamedDbIndex;
import com.torodb.backend.tables.MetaCollectionTable;
import com.torodb.backend.tables.MetaDatabaseTable;
import com.torodb.backend.tables.MetaDocPartTable;
import com.torodb.backend.tables.MetaFieldTable;
import com.torodb.backend.tables.records.MetaCollectionRecord;
import com.torodb.backend.tables.records.MetaDatabaseRecord;
import com.torodb.backend.tables.records.MetaDocPartRecord;
import com.torodb.backend.tables.records.MetaFieldRecord;

public interface ReadMetaDataInterface {
    @Nonnull <R extends MetaDatabaseRecord> MetaDatabaseTable<R> getMetaDatabaseTable();
    @Nonnull <R extends MetaCollectionRecord> MetaCollectionTable<R> getMetaCollectionTable();
    @Nonnull <T, R extends MetaDocPartRecord<T>> MetaDocPartTable<T, R> getMetaDocPartTable();
    @Nonnull <T, R extends MetaFieldRecord<T>> MetaFieldTable<T, R> getMetaFieldTable();
    
    long getDatabaseSize(@Nonnull DSLContext dsl, @Nonnull String databaseName);
    Long getCollectionSize(@Nonnull DSLContext dsl, @Nonnull String schema, @Nonnull String collection);
    Long getDocumentsSize(@Nonnull DSLContext dsl, @Nonnull String schema, @Nonnull String collection);
    Long getIndexSize(@Nonnull DSLContext dsl, @Nonnull String schema, @Nonnull String collection, @Nonnull String index, 
            @Nonnull Set<NamedDbIndex> relatedDbIndexes, @Nonnull Map<String, Integer> relatedToroIndexes);
}
