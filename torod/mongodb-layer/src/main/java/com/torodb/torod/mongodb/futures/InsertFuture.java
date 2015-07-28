
package com.torodb.torod.mongodb.futures;

import com.eightkdata.mongowp.mongoserver.api.callback.WriteOpResult;
import com.eightkdata.mongowp.mongoserver.api.safe.impl.SimpleWriteOpResult;
import com.eightkdata.mongowp.mongoserver.protocol.MongoWP.ErrorCode;
import com.torodb.torod.core.connection.InsertResponse;
import java.util.concurrent.Future;

/**
 *
 */
public class InsertFuture extends ActionAndCommitFuture<WriteOpResult, InsertResponse> {

    public InsertFuture(Future<InsertResponse> insertFuture, Future<?> commitFuture) {
        super(insertFuture, commitFuture);
    }

    @Override
    public WriteOpResult transform(InsertResponse actionResult) {
        if (actionResult.isSuccess()) {
            return new SimpleWriteOpResult(ErrorCode.OK, null, //TODO: Fill replication info
            null //TODO: Fill shard info
            );
        }
        else {
            return new SimpleWriteOpResult(ErrorCode.OPERATION_FAILED, //TODO: modify Toro api to return error type indicator!
            null, //TODO: Fill replication info
            null //TODO: Fill shard info
            );
        }
    }

}
