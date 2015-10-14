
package com.torodb.torod.mongodb.futures;

import com.eightkdata.mongowp.mongoserver.api.safe.impl.DeleteOpResult;
import com.eightkdata.mongowp.mongoserver.pojos.OpTime;
import com.eightkdata.mongowp.mongoserver.protocol.MongoWP.ErrorCode;
import com.google.common.util.concurrent.ListenableFuture;
import com.torodb.torod.core.connection.DeleteResponse;

/**
 *
 */
public class DeleteFuture extends ActionAndCommitFuture<DeleteOpResult, DeleteResponse>{

    public DeleteFuture(OpTime optime, ListenableFuture<DeleteResponse> actionFuture, ListenableFuture<?> commitFuture) {
        super(optime, actionFuture, commitFuture);
    }

    @Override
    public DeleteOpResult transform(DeleteResponse actionResult) {
        long deletedDocsCounter = actionResult.getDeleted();
        ErrorCode error;
        String errorDesc;
        if (actionResult.isSuccess()) {
            error = ErrorCode.OK;
            errorDesc = null;
        }
        else {
            error = ErrorCode.OPERATION_FAILED; //TODO: modify Toro api to return error type indicator!
            errorDesc = "Error while deleting";
        }
        return new DeleteOpResult(deletedDocsCounter, error, errorDesc, null, null, getOptime());
    }


}
