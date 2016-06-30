package com.torodb.backend;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import org.jooq.DSLContext;

import com.torodb.backend.index.NamedDbIndex;
import com.torodb.backend.tables.MetaCollectionTable;
import com.torodb.backend.tables.MetaDatabaseTable;
import com.torodb.backend.tables.MetaDocPartTable;
import com.torodb.backend.tables.MetaFieldTable;
import com.torodb.backend.tables.MetaScalarTable;
import com.torodb.backend.tables.records.MetaCollectionRecord;
import com.torodb.backend.tables.records.MetaDatabaseRecord;
import com.torodb.backend.tables.records.MetaDocPartRecord;
import com.torodb.backend.tables.records.MetaFieldRecord;
import com.torodb.backend.tables.records.MetaScalarRecord;
import com.torodb.core.TableRef;
import com.torodb.core.transaction.metainf.MetaDocPart;

public interface MetaDataReadInterface {
    @Nonnull <R extends MetaDatabaseRecord> MetaDatabaseTable<R> getMetaDatabaseTable();
    @Nonnull <R extends MetaCollectionRecord> MetaCollectionTable<R> getMetaCollectionTable();
    @Nonnull <T, R extends MetaDocPartRecord<T>> MetaDocPartTable<T, R> getMetaDocPartTable();
    @Nonnull <T, R extends MetaFieldRecord<T>> MetaFieldTable<T, R> getMetaFieldTable();
    @Nonnull <T, R extends MetaScalarRecord<T>> MetaScalarTable<T, R> getMetaScalarTable();
    
    @Nonnull Collection<InternalField<?>> getInternalFields(@Nonnull MetaDocPart metaDocPart);
    @Nonnull Collection<InternalField<?>> getInternalFields(@Nonnull TableRef tableRef);
    @Nonnull Collection<InternalField<?>> getPrimaryKeyInternalFields(@Nonnull TableRef tableRef);
    @Nonnull Collection<InternalField<?>> getReferenceInternalFields(@Nonnull TableRef tableRef);
    @Nonnull Collection<InternalField<?>> getForeignInternalFields(@Nonnull TableRef tableRef);
    
    long getDatabaseSize(@Nonnull DSLContext dsl, @Nonnull String databaseName);
    Long getCollectionSize(@Nonnull DSLContext dsl, @Nonnull String schema, @Nonnull String collection);
    Long getDocumentsSize(@Nonnull DSLContext dsl, @Nonnull String schema, @Nonnull String collection);
    Long getIndexSize(@Nonnull DSLContext dsl, @Nonnull String schema, @Nonnull String collection, @Nonnull String index, 
            @Nonnull Set<NamedDbIndex> relatedDbIndexes, @Nonnull Map<String, Integer> relatedToroIndexes);
}
