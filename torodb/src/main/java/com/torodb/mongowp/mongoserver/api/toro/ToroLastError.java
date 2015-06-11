/*
 *     This file is part of ToroDB.
 *
 *     ToroDB is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Affero General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     ToroDB is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Affero General Public License for more details.
 *
 *     You should have received a copy of the GNU Affero General Public License
 *     along with ToroDB. If not, see <http://www.gnu.org/licenses/>.
 *
 *     Copyright (c) 2014, 8Kdata Technology
 *     
 */

package com.torodb.mongowp.mongoserver.api.toro;

import com.eightkdata.mongowp.messages.request.RequestOpCode;
import com.eightkdata.mongowp.mongoserver.api.QueryCommandProcessor.QueryCommand;
import com.eightkdata.mongowp.mongoserver.api.callback.LastError;
import com.eightkdata.mongowp.mongoserver.api.callback.MessageReplier;
import com.eightkdata.mongowp.mongoserver.api.commands.QueryAndWriteOperationsQueryCommand;
import com.eightkdata.mongowp.mongoserver.protocol.MongoWP;
import com.eightkdata.nettybson.api.BSONDocument;
import com.eightkdata.nettybson.mongodriver.MongoBSONDocument;
import com.mongodb.WriteConcern;
import com.torodb.torod.core.connection.DeleteResponse;
import com.torodb.torod.core.connection.InsertResponse;
import com.torodb.torod.core.connection.UpdateResponse;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 */
public class ToroLastError implements LastError {
	
    private static final Logger LOGGER
            = LoggerFactory.getLogger(ToroLastError.class);
	@Nonnull private final RequestOpCode requestOpCode;
	private final QueryCommand queryCommand;
	private final Future<?> futureOperationResponse;
	private final Future<?> futureCommitResponse;
	private final boolean error;
	private final MongoWP.ErrorCode errorCode;

	public ToroLastError(@Nonnull RequestOpCode requestOpCode,
			QueryCommand queryCommand,
			Future<?> futureOperationResponse, Future<?> futureCommitResponse, 
			boolean error, MongoWP.ErrorCode errorCode) {
		this.requestOpCode = requestOpCode;
		this.queryCommand = queryCommand;
		this.futureOperationResponse = futureOperationResponse;
		this.futureCommitResponse = futureCommitResponse;
		this.error = error;
		this.errorCode = errorCode;
	}


	@Override
	public void getLastError(@Nonnull Object w, boolean j, boolean fsync, int wtimeout, MessageReplier messageReplier) throws Exception {
		Map<String, Object> keyValues = new HashMap<String, Object>();
		
		WriteConcern writeConcern = getWriteConcern(w, j, fsync, wtimeout);
		LastErrorResult lastErrorResult = new LastErrorResult(this);
		
		if (!lastErrorResult.error) {
			switch (requestOpCode) {
			case OP_QUERY:
				if (queryCommand != null) {
					if(queryCommand.equals(QueryAndWriteOperationsQueryCommand.insert)) {
						getLastInsertError(writeConcern, lastErrorResult);
					} else
					if(queryCommand.equals(QueryAndWriteOperationsQueryCommand.update)) {
						getLastUpdateError(writeConcern, lastErrorResult);
					} else
					if(queryCommand.equals(QueryAndWriteOperationsQueryCommand.delete)) {
						getLastDeleteError(writeConcern, lastErrorResult);
					}
				}
				break;
			case OP_INSERT:
				getLastInsertError(writeConcern, lastErrorResult);
				break;
			case OP_UPDATE:
				getLastUpdateError(writeConcern, lastErrorResult);
				break;
			case OP_DELETE:
				getLastDeleteError(writeConcern, lastErrorResult);
				break;
			case OP_GET_MORE:
			case OP_KILL_CURSORS:
			case OP_MSG:
			case RESERVED:
				break;
			}
		}
		lastErrorResult.wtime = System.currentTimeMillis() - lastErrorResult.wtime;
		
		keyValues.put("ok", lastErrorResult.error ? MongoWP.KO : MongoWP.OK);
        if (error) { //TODO: Check if we want to use this or lastErrorResult.error
            keyValues.put("err", lastErrorResult.errorCode.getErrorMessage());
			keyValues.put("code", lastErrorResult.errorCode.getErrorCode());
		}
		keyValues.put("connectionId", messageReplier.getConnectionId());
		//TODO: keyValues.put("lastOp", ???);
		//TODO: keyValues.put("shards", ???);
		//TODO: keyValues.put("singleShard", ???);
		
		keyValues.put("n", lastErrorResult.n);
		keyValues.put("updatedExisting", lastErrorResult.updatedExisting);
		keyValues.put("upserted", lastErrorResult.upserted);
		keyValues.put("wnote", lastErrorResult.writeConcernError?MongoWP.OK:MongoWP.KO);
		//TODO: Implement timeout
		keyValues.put("wtimeout", false);
		//TODO: keyValues.put("waited", wtime);
		keyValues.put("wtime", (int) lastErrorResult.wtime);
		
		//TODO: Add undocumented "syncMillis" and "writtenTo"
		
		BSONDocument reply = new MongoBSONDocument(keyValues);
		messageReplier.replyMessageNoCursor(reply);
	}

	private void getLastInsertError(WriteConcern writeConcern,
			LastErrorResult lastErrorResult) throws InterruptedException,
			ExecutionException {
		if (futureOperationResponse == null && futureCommitResponse == null) {
			return;
		}
		if (writeConcern.getW() > 0) {
			try {
				if (futureOperationResponse != null) {
                    if (futureOperationResponse.isDone()  || futureOperationResponse.isCancelled()) {
                        InsertResponse insertResponse
                                = (InsertResponse) futureOperationResponse.get();
                        lastErrorResult.error = !insertResponse.isSuccess();
                        if (lastErrorResult.error) {
                            lastErrorResult.errorCode = MongoWP.ErrorCode.INTERNAL_ERROR;
                        }
                        else {
                            lastErrorResult.n = insertResponse.getInsertedSize();
                        }
                    }
                    futureOperationResponse.get();
                }
			} catch(InterruptedException exception) {
                LOGGER.debug("Exception while last error was calculated", exception);
				lastErrorResult.error = true;
			}
            catch (ExecutionException exception) {
                LOGGER.debug("Exception while last error was calculated", exception);
                lastErrorResult.error = true;
            }
		}
	}

	private void getLastUpdateError(WriteConcern writeConcern,
			LastErrorResult lastErrorResult) throws InterruptedException,
			ExecutionException {
		if (futureOperationResponse == null || futureCommitResponse == null) {
			return;
		}
		if (writeConcern.getW() > 0) {
			try {
				futureOperationResponse.get();
				futureCommitResponse.get();
			} catch(Exception exception) {
				lastErrorResult.error = true;
			}
		}
		if (futureOperationResponse.isDone() || futureOperationResponse.isCancelled()) {
			UpdateResponse updateResponse = (UpdateResponse) futureOperationResponse.get();
			lastErrorResult.error = !updateResponse.getErrors().isEmpty();
			if (lastErrorResult.error) {
				lastErrorResult.errorCode = MongoWP.ErrorCode.INTERNAL_ERROR;
			} else {
				lastErrorResult.n = updateResponse.getModified();
                int modifiedCount = updateResponse.getModified() - updateResponse.getInsertedDocuments().size();
				lastErrorResult.updatedExisting = modifiedCount > 0;
				lastErrorResult.upserted = updateResponse.getInsertedDocuments().size();
			}
		}
	}

	private void getLastDeleteError(WriteConcern writeConcern,
			LastErrorResult lastErrorResult) throws InterruptedException,
			ExecutionException {
		if (futureOperationResponse == null || futureCommitResponse == null) {
			return;
		}
		if (writeConcern.getW() > 0) {
			try {
				futureOperationResponse.get();
				futureCommitResponse.get();
			} catch(Exception exception) {
				lastErrorResult.error = true;
			}
		}
		if (futureOperationResponse.isDone() || futureOperationResponse.isCancelled()) {
			DeleteResponse deleteResponse = (DeleteResponse) futureOperationResponse.get();
			lastErrorResult.error = !deleteResponse.isSuccess();
			if (lastErrorResult.error) {
				lastErrorResult.errorCode = MongoWP.ErrorCode.INTERNAL_ERROR;
			} else {
				lastErrorResult.n = deleteResponse.getDeleted();
			}
		}
	}

	@SuppressFBWarnings("DLS_DEAD_LOCAL_STORE")
	private WriteConcern getWriteConcern(Object w, boolean j, boolean fsync, int wtimeout) {
		WriteConcern writeConcern = WriteConcern.ACKNOWLEDGED;

        if (w instanceof Number) {
			if (((Number) w).intValue() <= 1 && wtimeout > 0) {
				throw new IllegalArgumentException("wtimeout cannot be grater than 0 for w <= 1");
			}
			
			writeConcern = new WriteConcern(((Number) w).intValue(), wtimeout, fsync, j);
		} else
   		if (w instanceof String && w.equals("majority")) {
   			if (wtimeout > 0) {
   				throw new IllegalArgumentException("wtimeout cannot be grater than 0 for w <= 1");
   			}
   			
   			writeConcern = new WriteConcern.Majority(wtimeout, fsync, j);
		} else {
			throw new IllegalArgumentException("w:" + w + " is not supported");
		}

		return writeConcern;
	}

    private static class LastErrorResult {

        public long wtime = System.currentTimeMillis();
        public int n = 0;
        public boolean updatedExisting = false;
        public int upserted = 0;
        public boolean error;
        public MongoWP.ErrorCode errorCode;
        public boolean writeConcernError = false;

        LastErrorResult(ToroLastError toroLastError) {
            error = toroLastError.error;
            errorCode = toroLastError.errorCode;
        }
    }
}
