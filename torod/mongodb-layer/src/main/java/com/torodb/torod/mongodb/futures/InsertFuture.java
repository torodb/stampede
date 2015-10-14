
package com.torodb.torod.mongodb.futures;

import com.eightkdata.mongowp.mongoserver.api.safe.impl.SimpleWriteOpResult;
import com.eightkdata.mongowp.mongoserver.callback.WriteOpResult;
import com.eightkdata.mongowp.mongoserver.pojos.OpTime;
import com.eightkdata.mongowp.mongoserver.protocol.MongoWP.ErrorCode;
import com.google.common.util.concurrent.ListenableFuture;
import com.torodb.torod.core.connection.InsertResponse;

/**
 *
 */
public class InsertFuture extends ActionAndCommitFuture<WriteOpResult, InsertResponse> {

    public InsertFuture(
            OpTime optime,
            ListenableFuture<InsertResponse> insertFuture,
            ListenableFuture<?> commitFuture) {
        super(optime, insertFuture, commitFuture);
    }

    @Override
    public WriteOpResult transform(InsertResponse actionResult) {
        if (actionResult.isSuccess()) {
            return new SimpleWriteOpResult(
                    ErrorCode.OK,
                    null, //TODO: Fill replication info
                    null, //TODO: Fill shard info
                    getOptime()
            );
        }
        else {
            return new SimpleWriteOpResult(
                    ErrorCode.OPERATION_FAILED, //TODO: modify Toro api to return error type indicator!
                    null, //TODO: Fill replication info
                    null, //TODO: Fill shard info
                    getOptime()
            );
        }
    }

}
