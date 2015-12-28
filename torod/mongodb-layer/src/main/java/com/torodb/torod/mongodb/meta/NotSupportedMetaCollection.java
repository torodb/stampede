
package com.torodb.torod.mongodb.meta;

import com.eightkdata.mongowp.mongoserver.protocol.exceptions.NamespaceNotFoundException;
import com.torodb.torod.core.connection.ToroConnection;
import com.torodb.torod.core.subdocument.ToroDocument;
import java.util.List;

/**
 *
 */
public class NotSupportedMetaCollection extends MetaCollection {

    public NotSupportedMetaCollection(String databaseName, String collectionName) {
        super(databaseName, collectionName);
    }

    @Override
    public List<ToroDocument> queryAllDocuments(ToroConnection toroConnection) throws NamespaceNotFoundException {
        throw new NamespaceNotFoundException(getDatabaseName(), getCollectionName());
    }

    @Override
    public long count(ToroConnection connection) throws NamespaceNotFoundException {
        throw new NamespaceNotFoundException(getDatabaseName(), getCollectionName());
    }

    @Override
    public boolean isCapped() throws NamespaceNotFoundException {
        throw new NamespaceNotFoundException(getDatabaseName(), getCollectionName());
    }

    @Override
    public Number getMaxIfCapped() throws NamespaceNotFoundException {
        throw new NamespaceNotFoundException(getDatabaseName(), getCollectionName());
    }

}
