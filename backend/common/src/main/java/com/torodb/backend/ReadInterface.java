package com.torodb.backend;

import java.sql.SQLException;
import java.util.Collection;

import javax.annotation.Nonnull;

import org.jooq.DSLContext;

import com.torodb.core.backend.DidCursor;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.core.transaction.metainf.MetaField;
import com.torodb.kvdocument.values.KVValue;

public interface ReadInterface {

    @Nonnull
    DidCursor getCollectionDidsWithFieldEqualsTo(@Nonnull DSLContext dsl, @Nonnull MetaDatabase metaDatabase,
            @Nonnull MetaCollection metaCol, @Nonnull MetaDocPart metaDocPart,
            @Nonnull MetaField metaField, @Nonnull KVValue<?> value)
            throws SQLException;

    long countAll(@Nonnull DSLContext dsl, @Nonnull MetaDatabase database, @Nonnull MetaCollection collection);

    @Nonnull
    DidCursor getAllCollectionDids(@Nonnull DSLContext dsl, @Nonnull MetaDatabase metaDatabase, @Nonnull MetaCollection metaCollection)
            throws SQLException;

    @Nonnull
    DocPartResultBatch getCollectionResultSets(@Nonnull DSLContext dsl, @Nonnull MetaDatabase metaDatabase, @Nonnull MetaCollection metaCollection,
            @Nonnull DidCursor didCursor, int maxSize) throws SQLException;

    @Nonnull
    DocPartResultBatch getCollectionResultSets(@Nonnull DSLContext dsl, @Nonnull MetaDatabase metaDatabase, @Nonnull MetaCollection metaCollection,
            @Nonnull Collection<Integer> dids) throws SQLException;

    int getLastRowIdUsed(@Nonnull DSLContext dsl, @Nonnull MetaDatabase metaDatabase, @Nonnull MetaCollection metaCollection, @Nonnull MetaDocPart metaDocPart);
}
