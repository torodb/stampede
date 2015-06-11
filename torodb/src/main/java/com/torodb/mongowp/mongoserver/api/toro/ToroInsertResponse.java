package com.torodb.mongowp.mongoserver.api.toro;

import com.eightkdata.mongowp.mongoserver.api.QueryCommandProcessor;
import com.eightkdata.mongowp.mongoserver.api.callback.LastError;
import com.eightkdata.mongowp.mongoserver.api.callback.MessageReplier;
import com.eightkdata.mongowp.mongoserver.api.pojos.InsertResponse;
import com.eightkdata.mongowp.mongoserver.protocol.MongoWP;
import com.eightkdata.nettybson.mongodriver.MongoBSONDocument;
import com.google.common.collect.ImmutableList;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.types.BasicBSONList;

/**
 *
 */
public class ToroInsertResponse extends InsertResponse {

    public ToroInsertResponse(
            boolean ok,
            int n,
            ImmutableList<WriteError> writeErrors,
            ImmutableList<WriteConcernError> writeConcernErrors) {
        super(ok, n, writeErrors, writeConcernErrors);
    }

    @Override
    public LastError renderizeAsLastError() {
        return new LastError() {

            @Override
            public void getLastError(
                    Object w,
                    boolean j,
                    boolean fsync,
                    int wtimeout,
                    MessageReplier messageReplier) throws Exception {
                BasicBSONObject obj = new BasicBSONObject();
                obj.put("ok", isOk() ? MongoWP.OK : MongoWP.KO);
                obj.put("err", MongoWP.ErrorCode.INTERNAL_ERROR.getErrorMessage());
                obj.put("code", MongoWP.ErrorCode.INTERNAL_ERROR.getErrorCode());
                obj.put("connectionId", messageReplier.getConnectionId());
                //obj.put("lastOp", ); ???
                obj.put("n", getN());
                //obj.put("syncMillis", ); ???

                obj.put("wnote", false);
                obj.put("wtimeout", false);

                messageReplier.replyMessageNoCursor(new MongoBSONDocument((BSONObject) obj));
            }
        };
    }

    @Override
    public void renderize(MessageReplier messageReplier) {
        BasicBSONObject obj = new BasicBSONObject();
        obj.put("ok", isOk() ? MongoWP.OK : MongoWP.KO);
        obj.put("n", getN());

        if (!getWriteErrors().isEmpty()) {
            BasicBSONList bsonWriteErrors = new BasicBSONList();
            for (WriteError writeError : getWriteErrors()) {
                BasicBSONObject bsonWriteError = new BasicBSONObject();
                bsonWriteError.put("index", writeError.getIndex());
                bsonWriteError.put("code", writeError.getCode());
                bsonWriteError.put("errmsg", writeError.getErrmsg());

                bsonWriteErrors.add(bsonWriteError);
            }
            obj.put("writeErrors", bsonWriteErrors);
        }

        if (!getWriteConcernErrors().isEmpty()) {
            BasicBSONList bsonWriteConcernErrors = new BasicBSONList();
            for (WriteConcernError writeConcernError : getWriteConcernErrors()) {
                BasicBSONObject bsonWriteConcernError = new BasicBSONObject();
                bsonWriteConcernError.put("code", writeConcernError.getCode());
                bsonWriteConcernError.put("errmsg", writeConcernError.getErrmsg());

                bsonWriteConcernErrors.add(bsonWriteConcernError);
            }
            obj.put("writeConcernError", bsonWriteConcernErrors);
        }

        messageReplier.replyMessageNoCursor(new MongoBSONDocument((BSONObject) obj));
    }

    public static class Builder {

        private boolean ok;
        private int n;
        private final ImmutableList.Builder<WriteError> writeErrors = ImmutableList.builder();
        private final ImmutableList.Builder<WriteConcernError> writeConcernErrors = ImmutableList.builder();

        public boolean isOk() {
            return ok;
        }

        public void setOk(boolean ok) {
            this.ok = ok;
        }

        public int getN() {
            return n;
        }

        public void setN(int n) {
            this.n = n;
        }

        public ImmutableList.Builder<WriteError> getWriteErrors() {
            return writeErrors;
        }

        public ImmutableList.Builder<WriteConcernError> getWriteConcernErrors() {
            return writeConcernErrors;
        }

        public ToroInsertResponse build() {
            return new ToroInsertResponse(
                    ok,
                    n,
                    writeErrors.build(),
                    writeConcernErrors.build()
            );
        }
    }

}
