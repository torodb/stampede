
package com.torodb.torod.mongodb.futures;

import com.eightkdata.mongowp.mongoserver.api.safe.impl.UpdateOpResult;
import com.eightkdata.mongowp.mongoserver.pojos.OpTime;
import com.eightkdata.mongowp.mongoserver.protocol.MongoWP.ErrorCode;
import com.google.common.util.concurrent.ListenableFuture;
import com.torodb.torod.core.connection.UpdateResponse;

/**
 *
 */
public class UpdateFuture extends ActionAndCommitFuture<UpdateOpResult, UpdateResponse>{

    public UpdateFuture(OpTime optime, ListenableFuture<UpdateResponse> actionFuture, ListenableFuture<?> commitFuture) {
        super(optime, actionFuture, commitFuture);
    }

    @Override
    public UpdateOpResult transform(UpdateResponse actionResult) {
        boolean updateObjects = actionResult.getModified() > 0;
        ErrorCode error;
        String desc;
        if (actionResult.getErrors().isEmpty()) {
            error = ErrorCode.OK;
            desc = null;
        }
        else {
            error = ErrorCode.OPERATION_FAILED;  //TODO: modify Toro api to return an error type indicator!
            desc = "Error while updating";
        }

        return new UpdateOpResult(
                actionResult.getCandidates(),
                actionResult.getModified(),
                updateObjects,
                error,
                desc,
                null,
                null,
                getOptime()
        );
    }

}
