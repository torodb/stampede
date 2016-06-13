package com.torodb.backend.interfaces;

import java.sql.ResultSet;
import java.sql.SQLException;

import javax.annotation.Nonnull;

import org.jooq.DSLContext;

import com.torodb.core.d2r.DocPartResults;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDatabase;

public interface ReadInterface {
    @Nonnull DocPartResults<ResultSet> getCollectionResultSets(@Nonnull DSLContext dsl, @Nonnull MetaDatabase metaDatabase, @Nonnull MetaCollection metaCollection, 
            @Nonnull Integer[] requestedDocs) throws SQLException;
}
