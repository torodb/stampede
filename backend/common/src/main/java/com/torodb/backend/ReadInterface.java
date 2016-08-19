package com.torodb.backend;

import com.google.common.collect.Multimap;
import java.sql.SQLException;
import java.util.Collection;

import javax.annotation.Nonnull;

import org.jooq.DSLContext;

import com.torodb.core.cursors.Cursor;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.core.transaction.metainf.MetaField;
import com.torodb.kvdocument.values.KVValue;

public interface ReadInterface {

    @Nonnull
    Cursor<Integer> getCollectionDidsWithFieldEqualsTo(@Nonnull DSLContext dsl, @Nonnull MetaDatabase metaDatabase,
            @Nonnull MetaCollection metaCol, @Nonnull MetaDocPart metaDocPart,
            @Nonnull MetaField metaField, @Nonnull KVValue<?> value)
            throws SQLException;

    @Nonnull
    public Cursor<Integer> getCollectionDidsWithFieldsIn(DSLContext dsl, MetaDatabase metaDatabase,
            MetaCollection metaCol, MetaDocPart metaDocPart, Multimap<MetaField, KVValue<?>> valuesMap)
            throws SQLException;

    long countAll(@Nonnull DSLContext dsl, @Nonnull MetaDatabase database, @Nonnull MetaCollection collection);

    @Nonnull
    Cursor<Integer> getAllCollectionDids(@Nonnull DSLContext dsl, @Nonnull MetaDatabase metaDatabase, @Nonnull MetaCollection metaCollection)
            throws SQLException;

    @Nonnull
    DocPartResultBatch getCollectionResultSets(@Nonnull DSLContext dsl, @Nonnull MetaDatabase metaDatabase, @Nonnull MetaCollection metaCollection,
            @Nonnull Cursor<Integer> didCursor, int maxSize) throws SQLException;

    @Nonnull
    DocPartResultBatch getCollectionResultSets(@Nonnull DSLContext dsl, @Nonnull MetaDatabase metaDatabase, @Nonnull MetaCollection metaCollection,
            @Nonnull Collection<Integer> dids) throws SQLException;

    int getLastRowIdUsed(@Nonnull DSLContext dsl, @Nonnull MetaDatabase metaDatabase, @Nonnull MetaCollection metaCollection, @Nonnull MetaDocPart metaDocPart);
}
