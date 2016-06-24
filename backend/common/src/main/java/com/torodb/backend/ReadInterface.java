package com.torodb.backend;

import java.sql.ResultSet;
import java.sql.SQLException;

import javax.annotation.Nonnull;

import org.jooq.DSLContext;

import com.torodb.core.backend.DidCursor;
import com.torodb.core.d2r.DocPartResults;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.core.transaction.metainf.MetaField;
import com.torodb.kvdocument.values.KVValue;

public interface ReadInterface {
    @Nonnull ResultSet getCollectionDidsWithFieldEqualsTo(@Nonnull DSLContext dsl, @Nonnull MetaDatabase metaDatabase,  
            @Nonnull MetaDocPart metaDocPart, @Nonnull MetaField metaField, @Nonnull KVValue<?> value) throws SQLException;
    @Nonnull ResultSet getAllCollectionDids(@Nonnull DSLContext dsl, @Nonnull MetaDatabase metaDatabase, @Nonnull MetaDocPart metaDocPart) throws SQLException;
    @Nonnull DocPartResults<ResultSet> getCollectionResultSets(@Nonnull DSLContext dsl, @Nonnull MetaDatabase metaDatabase, @Nonnull MetaCollection metaCollection, 
            @Nonnull DidCursor didCursor, int maxSize) throws SQLException;
    int getLastRowIdUsed(@Nonnull DSLContext dsl, @Nonnull MetaDatabase metaDatabase, @Nonnull MetaCollection metaCollection, @Nonnull MetaDocPart metaDocPart);
}
