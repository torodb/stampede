package com.torodb.mongowp.mongoserver.api.toro;

import com.eightkdata.mongowp.mongoserver.api.callback.LastError;
import com.eightkdata.mongowp.mongoserver.api.callback.MessageReplier;
import com.eightkdata.mongowp.mongoserver.api.commands.pojos.InsertReply;
import com.eightkdata.mongowp.mongoserver.protocol.MongoWP;
import com.eightkdata.nettybson.mongodriver.MongoBSONDocument;
import com.google.common.collect.ImmutableList;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.types.BasicBSONList;

/**
 *
 */
public class ToroInsertReply extends InsertReply {

    public ToroInsertReply(int n) {
        super(n);
    }

    public ToroInsertReply(
            int n, 
            ImmutableList<WriteError> writeErrors, 
            ImmutableList<WriteConcernError> writeConcernErrors, 
            MongoWP.ErrorCode errorCode, 
            String errorMessage) {
        super(n, writeErrors, writeConcernErrors, errorCode, errorMessage);
    }

    public ToroInsertReply(
            int n, 
            ImmutableList<WriteError> writeErrors, 
            ImmutableList<WriteConcernError> writeConcernErrors, 
            MongoWP.ErrorCode errorCode, 
            Object... args) {
        super(n, writeErrors, writeConcernErrors, errorCode, args);
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
                toMap(obj);
                obj.put("err", getErrorMessage());
                obj.put("code", getErrorCode().getErrorCode());
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
    public void reply(MessageReplier messageReplier) {
        BasicBSONObject obj = new BasicBSONObject();
        toMap(obj);
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

        private MongoWP.ErrorCode errorCode;
        private int n;
        private final ImmutableList.Builder<WriteError> writeErrors = ImmutableList.builder();
        private final ImmutableList.Builder<WriteConcernError> writeConcernErrors = ImmutableList.builder();

        public MongoWP.ErrorCode getErrorCode() {
            return errorCode;
        }

        public Builder setErrorCode(MongoWP.ErrorCode errorCode) {
            this.errorCode = errorCode;
            return this;
        }

        public int getN() {
            return n;
        }

        public Builder setN(int n) {
            this.n = n;
            return this;
        }

        public ImmutableList.Builder<WriteError> getWriteErrors() {
            return writeErrors;
        }

        public ImmutableList.Builder<WriteConcernError> getWriteConcernErrors() {
            return writeConcernErrors;
        }

        public ToroInsertReply build() {
            return new ToroInsertReply(
                    n,
                    writeErrors.build(),
                    writeConcernErrors.build(),
                    errorCode
            );
        }
    }

}
