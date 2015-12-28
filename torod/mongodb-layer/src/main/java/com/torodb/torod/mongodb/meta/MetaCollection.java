
package com.torodb.torod.mongodb.meta;

import com.eightkdata.mongowp.mongoserver.protocol.exceptions.MongoException;
import com.torodb.torod.core.connection.ToroConnection;
import com.torodb.torod.core.subdocument.ToroDocument;
import java.util.List;

/**
 *
 */
public abstract class MetaCollection {

    private final String collectionName;
    private final String databaseName;

    MetaCollection(
            String databaseName,
            String collectionName) {
        this.collectionName = collectionName;
        this.databaseName = databaseName;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public abstract List<ToroDocument> queryAllDocuments(ToroConnection connection) throws MongoException;

    public abstract long count(ToroConnection connection) throws MongoException;
    
    public abstract boolean isCapped() throws MongoException;
    
    public abstract Number getMaxIfCapped() throws MongoException;
}
