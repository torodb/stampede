
package com.torodb.torod.mongodb.crp;

import com.torodb.torod.mongodb.crp.CollectionRequestProcessor;
import com.eightkdata.mongowp.messages.request.DeleteMessage;
import com.eightkdata.mongowp.messages.request.InsertMessage;
import com.eightkdata.mongowp.messages.request.UpdateMessage;
import com.eightkdata.mongowp.mongoserver.api.safe.Request;
import com.eightkdata.mongowp.mongoserver.api.safe.impl.UpdateOpResult;
import com.eightkdata.mongowp.mongoserver.api.safe.pojos.QueryRequest;
import com.eightkdata.mongowp.mongoserver.callback.WriteOpResult;
import com.eightkdata.mongowp.mongoserver.protocol.exceptions.DatabaseNotFoundException;
import com.eightkdata.mongowp.mongoserver.protocol.exceptions.MongoException;
import com.google.common.util.concurrent.ListenableFuture;

/**
 *
 */
public class NotSupportedDatabaseCollection implements CollectionRequestProcessor {

    private final String database;

    public NotSupportedDatabaseCollection(String database) {
        this.database = database;
    }

    @Override
    public QueryResponse query(Request request, QueryRequest queryMessage) throws
            MongoException {
        throw new DatabaseNotFoundException(database);
    }

    @Override
    public ListenableFuture<? extends WriteOpResult> insert(Request request, InsertMessage insertMessage)
            throws MongoException {
        throw new DatabaseNotFoundException(database);
    }

    @Override
    public ListenableFuture<? extends UpdateOpResult> update(Request request, UpdateMessage deleteMessage)
            throws MongoException {
        throw new DatabaseNotFoundException(database);
    }

    @Override
    public ListenableFuture<? extends WriteOpResult> delete(Request request, DeleteMessage deleteMessage)
            throws MongoException {
        throw new DatabaseNotFoundException(database);
    }

}
