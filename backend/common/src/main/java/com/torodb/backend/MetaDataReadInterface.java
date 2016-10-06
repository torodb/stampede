package com.torodb.backend;

import java.util.Collection;

import javax.annotation.Nonnull;

import org.jooq.DSLContext;

import com.torodb.backend.tables.MetaCollectionTable;
import com.torodb.backend.tables.MetaDatabaseTable;
import com.torodb.backend.tables.MetaDocPartIndexTable;
import com.torodb.backend.tables.MetaDocPartTable;
import com.torodb.backend.tables.MetaFieldIndexTable;
import com.torodb.backend.tables.MetaFieldTable;
import com.torodb.backend.tables.MetaIndexFieldTable;
import com.torodb.backend.tables.MetaIndexTable;
import com.torodb.backend.tables.MetaScalarTable;
import com.torodb.backend.tables.records.MetaCollectionRecord;
import com.torodb.backend.tables.records.MetaDatabaseRecord;
import com.torodb.backend.tables.records.MetaDocPartIndexRecord;
import com.torodb.backend.tables.records.MetaDocPartRecord;
import com.torodb.backend.tables.records.MetaFieldIndexRecord;
import com.torodb.backend.tables.records.MetaFieldRecord;
import com.torodb.backend.tables.records.MetaIndexFieldRecord;
import com.torodb.backend.tables.records.MetaIndexRecord;
import com.torodb.backend.tables.records.MetaScalarRecord;
import com.torodb.core.TableRef;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MetaDocPart;

public interface MetaDataReadInterface {
    @Nonnull <R extends MetaDatabaseRecord> MetaDatabaseTable<R> getMetaDatabaseTable();
    @Nonnull <R extends MetaCollectionRecord> MetaCollectionTable<R> getMetaCollectionTable();
    @Nonnull <T, R extends MetaDocPartRecord<T>> MetaDocPartTable<T, R> getMetaDocPartTable();
    @Nonnull <T, R extends MetaFieldRecord<T>> MetaFieldTable<T, R> getMetaFieldTable();
    @Nonnull <T, R extends MetaScalarRecord<T>> MetaScalarTable<T, R> getMetaScalarTable();
    @Nonnull <T, R extends MetaDocPartIndexRecord<T>> MetaDocPartIndexTable<T, R> getMetaDocPartIndexTable();
    @Nonnull <T, R extends MetaFieldIndexRecord<T>> MetaFieldIndexTable<T, R> getMetaFieldIndexTable();
    @Nonnull <R extends MetaIndexRecord> MetaIndexTable<R> getMetaIndexTable();
    @Nonnull <T, R extends MetaIndexFieldRecord<T>> MetaIndexFieldTable<T, R> getMetaIndexFieldTable();
    
    @Nonnull Collection<InternalField<?>> getInternalFields(@Nonnull MetaDocPart metaDocPart);
    @Nonnull Collection<InternalField<?>> getInternalFields(@Nonnull TableRef tableRef);
    @Nonnull Collection<InternalField<?>> getPrimaryKeyInternalFields(@Nonnull TableRef tableRef);
    @Nonnull Collection<InternalField<?>> getReferenceInternalFields(@Nonnull TableRef tableRef);
    @Nonnull Collection<InternalField<?>> getForeignInternalFields(@Nonnull TableRef tableRef);
    @Nonnull Collection<InternalField<?>> getReadInternalFields(@Nonnull MetaDocPart metaDocPart);
    @Nonnull Collection<InternalField<?>> getReadInternalFields(@Nonnull TableRef tableRef);
    
    long getDatabaseSize(@Nonnull DSLContext dsl, @Nonnull MetaDatabase database);
    long getCollectionSize(@Nonnull DSLContext dsl, @Nonnull MetaDatabase database, @Nonnull MetaCollection collection);
    long getDocumentsSize(@Nonnull DSLContext dsl, @Nonnull MetaDatabase database, @Nonnull MetaCollection collection);
    Long getIndexSize(@Nonnull DSLContext dsl, @Nonnull MetaDatabase database, @Nonnull MetaCollection collection, @Nonnull String index);
}
