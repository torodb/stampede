
package com.torodb.torod.db.backends.executor;

import com.torodb.torod.core.ValueRow;
import com.torodb.torod.core.cursors.CursorId;
import com.torodb.torod.core.dbWrapper.Cursor;
import com.torodb.torod.core.dbWrapper.DbConnection;
import com.torodb.torod.core.dbWrapper.DbWrapper;
import com.torodb.torod.core.dbWrapper.exceptions.ImplementationDbException;
import com.torodb.torod.core.dbWrapper.exceptions.UserDbException;
import com.torodb.torod.core.exceptions.IllegalPathViewException;
import com.torodb.torod.core.exceptions.ToroImplementationException;
import com.torodb.torod.core.language.projection.Projection;
import com.torodb.torod.core.language.querycriteria.QueryCriteria;
import com.torodb.torod.core.pojos.CollectionMetainfo;
import com.torodb.torod.core.pojos.IndexedAttributes;
import com.torodb.torod.core.pojos.NamedToroIndex;
import com.torodb.torod.core.subdocument.SplitDocument;
import com.torodb.torod.core.subdocument.SubDocType;
import com.torodb.torod.core.subdocument.SubDocument;
import com.torodb.torod.core.subdocument.values.Value;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;
import javax.json.JsonObject;

/**
 *
 */
@NotThreadSafe
public class LazyDbWrapper implements DbWrapper {

    @Nonnull 
    private final DbWrapper delegate;

    public LazyDbWrapper(@Nonnull DbWrapper dbWrapper) {
        this.delegate = dbWrapper;
    }

    @Override
    public void initialize() throws ImplementationDbException {
        delegate.initialize();
    }
    
    @Override
    public DbConnection consumeSessionDbConnection() throws
            ImplementationDbException {
        return new LazySessionDbConnection();
    }

    @Override
    public DbConnection getSystemDbConnection() throws ImplementationDbException {
        return delegate.getSystemDbConnection();
    }

    @Override
    public Cursor openGlobalCursor(
            String collection, 
            CursorId cursorId, 
            QueryCriteria filter, 
            Projection projection, 
            int maxResults)
            throws ImplementationDbException, UserDbException {
        return delegate.openGlobalCursor(collection, cursorId, filter, projection, maxResults);
    }

    @Override
    public Cursor getGlobalCursor(CursorId cursorId) throws
            IllegalArgumentException {
        return delegate.getGlobalCursor(cursorId);
    }
    
    @NotThreadSafe
    private class LazySessionDbConnection implements DbConnection {
        private DbConnection delegate = null;
        
        private DbConnection getDelegate() throws ImplementationDbException {
            if (delegate == null) {
                delegate = LazyDbWrapper.this.delegate.consumeSessionDbConnection();
            }
            return delegate;
        }

        @Override
        public void close() throws ImplementationDbException, UserDbException {
            if (delegate != null) {
                delegate.close();
            }
        }

        @Override
        public void commit() throws ImplementationDbException, UserDbException {
            if (delegate != null) {
                delegate.commit();
            }
        }

        @Override
        public void rollback() throws ImplementationDbException {
            if (delegate != null) {
                delegate.rollback();
            }
        }

        @Override
        public void createCollection(
                String collectionName, 
                String schemaName, 
                JsonObject other) throws ImplementationDbException {
            getDelegate().createCollection(collectionName, schemaName, other);
        }

        @Override
        public void createSubDocTypeTable(String collection, SubDocType subDocType)
                throws ImplementationDbException {
            getDelegate().createSubDocTypeTable(collection, subDocType);
        }

        @Override
        public void reserveDocIds(String collection, int idsToReserve) throws
                ImplementationDbException {
            getDelegate().reserveDocIds(collection, idsToReserve);
        }

        @Override
        public void insertRootDocuments(String collection, Collection<SplitDocument> docs)
                throws ImplementationDbException, UserDbException {
            getDelegate().insertRootDocuments(collection, docs);
        }

        @Override
        public void insertSubdocuments(
                String collection, 
                SubDocType subDocType, 
                Iterator<? extends SubDocument> subDocuments) {
            try {
                getDelegate().insertSubdocuments(collection, subDocType, subDocuments);
            }
            catch (ImplementationDbException ex) {
                throw new ToroImplementationException(ex);
            }
        }

        @Override
        public Map<String, Integer> findCollections() throws
                ImplementationDbException {
            return getDelegate().findCollections();
        }

        @Override
        public int delete(String collection, QueryCriteria condition, boolean justOne)
                throws ImplementationDbException, UserDbException {
            return getDelegate().delete(collection, condition, justOne);
        }

        @Override
        public long getDatabaseSize() {
            try {
                return getDelegate().getDatabaseSize();
            }
            catch (ImplementationDbException ex) {
                throw new ToroImplementationException(ex);
            }
        }

        @Override
        public void dropCollection(String collection) {
            try {
                getDelegate().dropCollection(collection);
            }
            catch (ImplementationDbException ex) {
                throw new ToroImplementationException(ex);
            }
        }

        @Override
        public NamedToroIndex createIndex(
                String collection, 
                String indexName, 
                IndexedAttributes attributes, 
                boolean unique, 
                boolean blocking) {
            try {
                return getDelegate().createIndex(
                        collection, 
                        indexName, 
                        attributes, 
                        unique, 
                        blocking
                );
            } catch (ImplementationDbException ex) {
                throw new ToroImplementationException(ex);
            }
        }

        @Override
        public boolean dropIndex(String collection, String indexName) {
            try {
                return getDelegate().dropIndex(collection, indexName);   
            }
            catch (ImplementationDbException ex) {
                throw new ToroImplementationException(ex);
            }
        }

        @Override
        public Collection<? extends NamedToroIndex> getIndexes(String collection) {
            try {
                return getDelegate().getIndexes(collection);
            }
            catch (ImplementationDbException ex) {
                throw new ToroImplementationException(ex);
            }
        }

        @Override
        public Integer count(String collection, QueryCriteria query) {
            try {
                return getDelegate().count(collection, query);
            }
            catch (ImplementationDbException ex) {
                throw new ToroImplementationException(ex);
            }
        }

        @Override
        public Long getCollectionSize(String collection) {
            try {
                return getDelegate().getCollectionSize(collection);
            }
            catch (ImplementationDbException ex) {
                throw new ToroImplementationException(ex);
            }
        }

        @Override
        public Long getIndexSize(String collection, String index) {
            try {
                return getDelegate().getIndexSize(collection, index);
            }
            catch (ImplementationDbException ex) {
                throw new ToroImplementationException(ex);
            }
        }

        @Override
        public Long getDocumentsSize(String collection) {
            try {
                return getDelegate().getDocumentsSize(collection);
            }
            catch (ImplementationDbException ex) {
                throw new ToroImplementationException(ex);
            }
        }

        @Override
        public Integer createPathViews(String collection) throws IllegalPathViewException {
            try {
                return getDelegate().createPathViews(collection);
            } catch (ImplementationDbException ex) {
                throw new ToroImplementationException(ex);
            }
        }

        @Override
        public void dropPathViews(String collection) throws
                IllegalPathViewException {
            try {
                getDelegate().dropPathViews(collection);
            } catch (ImplementationDbException ex) {
                throw new ToroImplementationException(ex);
            }
        }

        @Override
        public Iterator<ValueRow<Value>> select(String query) {
            try {
                return getDelegate().select(query);
            } catch (ImplementationDbException ex) {
                throw new ToroImplementationException(ex);
            }
        }

        @Override
        public List<CollectionMetainfo> getCollectionsMetainfo() {
            try {
                return getDelegate().getCollectionsMetainfo();
            }
            catch (ImplementationDbException ex) {
                throw new ToroImplementationException(ex);
            }
        }
    }
}
