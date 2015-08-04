
package com.torodb.torod.mongodb.futures;

import com.eightkdata.mongowp.mongoserver.api.safe.impl.UpdateOpResult;
import com.eightkdata.mongowp.mongoserver.pojos.OpTime;
import com.eightkdata.mongowp.mongoserver.protocol.MongoWP.ErrorCode;
import com.torodb.torod.core.connection.UpdateResponse;
import java.util.concurrent.Future;

/**
 *
 */
public class UpdateFuture extends ActionAndCommitFuture<UpdateOpResult, UpdateResponse>{

    public UpdateFuture(OpTime optime, Future<UpdateResponse> actionFuture, Future<?> commitFuture) {
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
            error = ErrorCode.OPERATION_FAILED;  //TODO: modify Toro api to return error type indicator!
            desc = "Error while updating";
        }

        return new UpdateOpResult(
                actionResult.getCandidates(),
                updateObjects,
                error,
                desc,
                null,
                null,
                getOptime()
        );
    }

}
