package com.torodb.backend.interfaces;

import com.torodb.core.d2r.DocPartResults;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MetaDocPart;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import javax.annotation.Nonnull;
import org.jooq.DSLContext;

public interface ReadInterface {
    Connection createReadOnlyConnection();

    @Nonnull DocPartResults<ResultSet> getCollectionResultSets(@Nonnull DSLContext dsl, @Nonnull MetaDatabase metaDatabase, @Nonnull MetaCollection metaCollection, 
            @Nonnull Collection<Integer> requestedDocs) throws SQLException;
    int getLastRowIdUsed(@Nonnull DSLContext dsl, @Nonnull MetaDatabase metaDatabase, @Nonnull MetaCollection metaCollection, @Nonnull MetaDocPart metaDocPart);
}
