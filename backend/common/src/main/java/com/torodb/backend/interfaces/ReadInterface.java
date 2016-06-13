package com.torodb.backend.interfaces;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Comparator;

import javax.annotation.Nonnull;

import org.jooq.DSLContext;

import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MetaDocPart;

public interface ReadInterface {
    @Nonnull Collection<DocPartResultSet> getCollectionResultSets(@Nonnull DSLContext dsl, @Nonnull MetaDatabase metaDatabase, @Nonnull MetaCollection metaCollection, 
            @Nonnull Integer[] requestedDocs) throws SQLException;
    
    public static class DocPartResultSet {
        private final MetaDocPart metaDocPart;
        private final ResultSet resultSet;
        
        public DocPartResultSet(MetaDocPart metaDocPart, ResultSet resultSet) {
            super();
            this.metaDocPart = metaDocPart;
            this.resultSet = resultSet;
        }
        
        public MetaDocPart getMetaDocPart() {
            return metaDocPart;
        }
        public ResultSet getResultSet() {
            return resultSet;
        }
    }
}
