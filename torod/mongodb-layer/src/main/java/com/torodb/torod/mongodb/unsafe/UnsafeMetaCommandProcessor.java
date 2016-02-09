
package com.torodb.torod.mongodb.unsafe;

import com.eightkdata.mongowp.WriteConcern;
import com.eightkdata.mongowp.bson.BsonDocument;
import com.eightkdata.mongowp.server.api.deprecated.MetaCommandProcessor;
import com.eightkdata.mongowp.server.api.deprecated.pojos.InsertResponse;
import io.netty.util.AttributeMap;
import java.util.List;
import java.util.concurrent.Future;

/**
 *
 */
public class UnsafeMetaCommandProcessor extends MetaCommandProcessor {

    @Override
    protected Iterable<BsonDocument> queryNamespaces(String database, AttributeMap attributeMap, BsonDocument query)
            throws Exception {
        throw new UnsupportedOperationException("Not supported.");
    }

    @Override
    protected Iterable<BsonDocument> queryIndexes(String database, AttributeMap attributeMap, BsonDocument query)
            throws Exception {
        throw new UnsupportedOperationException("Not supported.");
    }

    @Override
    protected Iterable<BsonDocument> queryProfile(String database, AttributeMap attributeMap, BsonDocument query)
            throws Exception {
        throw new UnsupportedOperationException("Not supported.");
    }

    @Override
    protected Iterable<BsonDocument> queryJS(String database, AttributeMap attributeMap, BsonDocument query)
            throws Exception {
        throw new UnsupportedOperationException("Not supported.");
    }

    @Override
    public Future<InsertResponse> insertIndex(AttributeMap attributeMap, List<BsonDocument> docsToInsert, boolean ordered, WriteConcern wc)
            throws Exception {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public Future<InsertResponse> insertNamespace(AttributeMap attributeMap, List<BsonDocument> docsToInsert, boolean ordered, WriteConcern wc)
            throws Exception {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public Future<InsertResponse> insertProfile(AttributeMap attributeMap, List<BsonDocument> docsToInsert, boolean ordered, WriteConcern wc)
            throws Exception {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public Future<InsertResponse> insertJS(AttributeMap attributeMap, List<BsonDocument> docsToInsert, boolean ordered, WriteConcern wc)
            throws Exception {
        throw new UnsupportedOperationException("Not supported");
    }

}
