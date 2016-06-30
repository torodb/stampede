package com.torodb.backend;

import com.torodb.core.backend.DidCursor;
import com.torodb.core.d2r.DocPartResults;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.core.transaction.metainf.MetaField;
import com.torodb.kvdocument.values.KVValue;
import java.sql.ResultSet;
import java.sql.SQLException;
import javax.annotation.Nonnull;
import org.jooq.DSLContext;

public interface ReadInterface {

    @Nonnull
    DidCursor getCollectionDidsWithFieldEqualsTo(@Nonnull DSLContext dsl, @Nonnull MetaDatabase metaDatabase,
            @Nonnull MetaCollection metaCol, @Nonnull MetaDocPart metaDocPart,
            @Nonnull MetaField metaField, @Nonnull KVValue<?> value)
            throws SQLException;

    @Nonnull
    DidCursor getAllCollectionDids(@Nonnull DSLContext dsl, @Nonnull MetaDatabase metaDatabase, @Nonnull MetaCollection metaCollection)
            throws SQLException;

    @Nonnull
    DocPartResults<ResultSet> getCollectionResultSets(@Nonnull DSLContext dsl, @Nonnull MetaDatabase metaDatabase, @Nonnull MetaCollection metaCollection,
            @Nonnull DidCursor didCursor, int maxSize) throws SQLException;

    int getLastRowIdUsed(@Nonnull DSLContext dsl, @Nonnull MetaDatabase metaDatabase, @Nonnull MetaCollection metaCollection, @Nonnull MetaDocPart metaDocPart);
}
