
package com.torodb.torod;

import com.torodb.core.cursors.Cursor;
import com.torodb.core.cursors.ToroCursor;
import com.torodb.core.exceptions.user.CollectionNotFoundException;
import com.torodb.core.language.AttributeReference;
import com.torodb.kvdocument.values.KVValue;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;
import org.jooq.lambda.tuple.Tuple2;

/**
 *
 */
public interface TorodTransaction extends AutoCloseable {

    public boolean isClosed();

    public TorodConnection getConnection();

    public boolean existsCollection(String dbName, String colName);
    
    public List<String> getDatabases();

    public long getDatabaseSize(String dbName);
    
    public long countAll(String dbName, String colName);
    
    public long getCollectionSize(String dbName, String colName);
    
    public long getDocumentsSize(String dbName, String colName);

    public ToroCursor findAll(String dbName, String colName);

    public ToroCursor findByAttRef(String dbName, String colName, AttributeReference attRef, KVValue<?> value);

    public ToroCursor findByAttRefIn(String dbName, String colName, AttributeReference attRef, Collection<KVValue<?>> values);

    /**
     * Like {@link #findByAttRefIn(java.lang.String, java.lang.String, com.torodb.core.language.AttributeReference, java.util.Collection) },
     * but the returned cursor iterates on tuples whose first element is the did of the document
     * that fullfil the query and the second is the value of the referenced attribute that this
     * document has.
     *
     * @param dbName
     * @param colName
     * @param attRef
     * @param values
     * @return
     */
    public Cursor<Tuple2<Integer, KVValue<?>>> findByAttRefInProjection(String dbName,
            String colName, AttributeReference attRef, Collection<KVValue<?>> values);

    /**
     * Given a namespace and a cursor of dids, consumes the cursor and returns a new cursor that
     * fetch all iterated dids.
     * @param dbName
     * @param colName
     * @param didCursor
     * @return
     */
    public ToroCursor fetch(String dbName, String colName, Cursor<Integer> didCursor);

    public Stream<CollectionInfo> getCollectionsInfo(String dbName);

    public CollectionInfo getCollectionInfo(String dbName, String colName) throws CollectionNotFoundException;

    @Override
    public void close();

    public void rollback();

}
