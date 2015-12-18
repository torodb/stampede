
package com.torodb.torod.mongodb.unsafe;

import com.eightkdata.mongowp.mongoserver.api.MetaCommandProcessor;
import com.eightkdata.mongowp.mongoserver.api.commands.CollStatsReply;
import com.eightkdata.mongowp.mongoserver.api.commands.CollStatsRequest;
import com.eightkdata.mongowp.mongoserver.api.pojos.InsertResponse;
import com.google.common.base.Supplier;
import com.mongodb.WriteConcern;
import io.netty.util.AttributeMap;
import java.util.List;
import java.util.concurrent.Future;
import org.bson.BsonDocument;

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
    public CollStatsReply collStats(String database, CollStatsRequest request, Supplier<Iterable<BsonDocument>> docsSupplier)
            throws Exception {
        throw new UnsupportedOperationException("Not supported.");
    }

    @Override
    public Future<InsertResponse> insertIndex(AttributeMap attributeMap, List<BsonDocument> docsToInsert, boolean ordered, WriteConcern wc)
            throws Exception {
        throw new UnsupportedOperationException("Not supported.");
    }

    @Override
    public Future<InsertResponse> insertNamespace(AttributeMap attributeMap, List<BsonDocument> docsToInsert, boolean ordered, WriteConcern wc)
            throws Exception {
        throw new UnsupportedOperationException("Not supported.");
    }

    @Override
    public Future<InsertResponse> insertProfile(AttributeMap attributeMap, List<BsonDocument> docsToInsert, boolean ordered, WriteConcern wc)
            throws Exception {
        throw new UnsupportedOperationException("Not supported.");
    }

    @Override
    public Future<InsertResponse> insertJS(AttributeMap attributeMap, List<BsonDocument> docsToInsert, boolean ordered, WriteConcern wc)
            throws Exception {
        throw new UnsupportedOperationException("Not supported.");
    }

}
