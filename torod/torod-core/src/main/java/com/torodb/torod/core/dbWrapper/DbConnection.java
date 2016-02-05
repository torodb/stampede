/*
 *     This file is part of ToroDB.
 *
 *     ToroDB is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Affero General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     ToroDB is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Affero General Public License for more details.
 *
 *     You should have received a copy of the GNU Affero General Public License
 *     along with ToroDB. If not, see <http://www.gnu.org/licenses/>.
 *
 *     Copyright (c) 2014, 8Kdata Technology
 *     
 */

package com.torodb.torod.core.dbWrapper;

import com.google.common.annotations.Beta;
import com.torodb.torod.core.ValueRow;
import com.torodb.torod.core.dbWrapper.exceptions.ImplementationDbException;
import com.torodb.torod.core.dbWrapper.exceptions.UserDbException;
import com.torodb.torod.core.exceptions.IllegalPathViewException;
import com.torodb.torod.core.exceptions.UserToroException;
import com.torodb.torod.core.language.querycriteria.QueryCriteria;
import com.torodb.torod.core.pojos.CollectionMetainfo;
import com.torodb.torod.core.pojos.IndexedAttributes;
import com.torodb.torod.core.pojos.NamedToroIndex;
import com.torodb.torod.core.subdocument.SplitDocument;
import com.torodb.torod.core.subdocument.SubDocType;
import com.torodb.torod.core.subdocument.SubDocument;
import com.torodb.torod.core.subdocument.values.ScalarValue;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;
import javax.json.JsonObject;

/**
 *
 */
@ThreadSafe
public interface DbConnection extends AutoCloseable {

    /**
     * Close the connection.
     * <p>
     * This method must be called when the connection will not be used anymore
     * @throws com.torodb.torod.core.dbWrapper.exceptions.ImplementationDbException
     * @throws com.torodb.torod.core.dbWrapper.exceptions.UserDbException
     */
    @Override
    public void close() throws ImplementationDbException, UserDbException;
    
    public void commit() throws ImplementationDbException, UserDbException;

    public void rollback() throws ImplementationDbException;

    public void createCollection(
            @Nonnull String collectionName, 
            @Nullable String schemaName,
            @Nullable JsonObject other) throws ImplementationDbException;
    
    /**
     * Creates a table the table that will store elements of the given {@link SubDocType} in the database.
     * <p>
     * Clients must not call this function if this table already exists.
     * <p>
     * @param subDocType
     * @param collection
     * @throws ImplementationDbException if the 
     */
    public void createSubDocTypeTable(
            @Nonnull String collection,
            @Nonnull SubDocType subDocType) throws ImplementationDbException;

    
    public void reserveDocIds(
            @Nonnull String collection, 
            int idsToReserve) throws ImplementationDbException;

    /**
     * Declares the given documents as root documents.
     * <p>
     * This method declares the given documents as a root documents <b>BUT DOES NOT</b> insert their
     * subdocuments in the database. {@link #insertSubdocuments(java.lang.String, com.torodb.torod.core.subdocument.SubDocType, java.util.Iterator) 
     * } must be called to do that.
     * <p>
     * @param collection
     * @param docs
     * @throws com.torodb.torod.core.dbWrapper.exceptions.ImplementationDbException
     * @throws com.torodb.torod.core.dbWrapper.exceptions.UserDbException
     */
    public void insertRootDocuments(
            @Nonnull String collection, 
            @Nonnull Collection<SplitDocument> docs) throws ImplementationDbException, UserDbException;

    /**
     * Inserts all subdocuments of the given subtype contained in the given doc.
     * @param collection
     * @param subDocuments
     * @param subDocType
     */
    public void insertSubdocuments(
            @Nonnull String collection,
            @Nonnull SubDocType subDocType,
            @Nonnull Iterable<? extends SubDocument> subDocuments);

    /**
     * Returns a map that contains all collections in the database as keys and the last reserved doc id for each one as
     * value.
     * @return
     * @throws ImplementationDbException 
     */
    public Map<String, Integer> findCollections() throws ImplementationDbException;

    public int delete(
            @Nonnull String collection, 
            @Nonnull QueryCriteria condition, 
            boolean justOne
    ) throws ImplementationDbException, UserDbException;

    /**
     * @return the size (in bytes) of the database this connection is connected with.
     */
    public long getDatabaseSize();
    
    public void dropCollection(
            @Nonnull String collection
    );
    
    public NamedToroIndex createIndex(
            @Nonnull String collection,
            @Nonnull String indexName, 
            @Nonnull IndexedAttributes attributes,
            boolean unique,
            boolean blocking
    );
    
    public boolean dropIndex(
            String collection,
            String indexName
    );

    /**
     * Returns the collections metainformation of all collections on the 
     * database
     * <p>
     * This cursor must be closed onces it is not needed.
     * <p>
     * @return
     * @throws ImplementationDbException 
     * @throws UserDbException
     */
    @Nonnull
    public List<CollectionMetainfo> getCollectionsMetainfo();

    public Collection<? extends NamedToroIndex> getIndexes(String collection);

    public Integer count(String collection, QueryCriteria query);

    public Long getCollectionSize(String collection);
    
    public Long getDocumentsSize(String collection);

    public Long getIndexSize(String collection, String index);

    @Beta
    public Integer createPathViews(String collection) throws IllegalPathViewException;

    @Beta
    public void dropPathViews(String collection) throws IllegalPathViewException;

    public Iterator<ValueRow<ScalarValue<?>>> select(String query) throws UserToroException;

    @Immutable
    public static class Metainfo {
        public static final Metainfo READ_ONLY = new Metainfo(true);
        public static final Metainfo NOT_READ_ONLY = new Metainfo(false);

        private final boolean readOnly;

        public Metainfo(boolean readOnly) {
            this.readOnly = readOnly;
        }

        public boolean isReadOnly() {
            return readOnly;
        }
    }
}
