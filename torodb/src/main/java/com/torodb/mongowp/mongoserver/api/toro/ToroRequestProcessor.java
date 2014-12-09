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

import com.eightkdata.mongowp.messages.request.*;
import com.eightkdata.mongowp.messages.request.QueryMessage.Flag;
import com.eightkdata.mongowp.messages.response.ReplyMessage;
import com.eightkdata.mongowp.mongoserver.api.AbstractRequestProcessor;
import com.eightkdata.mongowp.mongoserver.api.QueryCommandProcessor;
import com.eightkdata.mongowp.mongoserver.api.QueryCommandProcessor.QueryCommand;
import com.eightkdata.mongowp.mongoserver.api.callback.LastError;
import com.eightkdata.mongowp.mongoserver.api.callback.MessageReplier;
import com.eightkdata.mongowp.mongoserver.protocol.MongoWP;
import com.eightkdata.mongowp.mongoserver.protocol.MongoWP.ErrorCode;
import com.eightkdata.nettybson.api.BSONDocument;
import com.eightkdata.nettybson.mongodriver.MongoBSONDocument;
import com.torodb.mongowp.mongoserver.api.toro.util.BSONDocuments;
import com.torodb.mongowp.mongoserver.api.toro.util.BSONToroDocument;
import com.torodb.torod.core.Torod;
import com.torodb.torod.core.WriteFailMode;
import com.torodb.torod.core.connection.*;
import com.torodb.torod.core.cursors.CursorId;
import com.torodb.torod.core.language.operations.DeleteOperation;
import com.torodb.torod.core.language.operations.UpdateOperation;
import com.torodb.torod.core.language.projection.Projection;
import com.torodb.torod.core.language.querycriteria.QueryCriteria;
import com.torodb.torod.core.language.update.UpdateAction;
import com.torodb.torod.core.subdocument.ToroDocument;
import com.torodb.translator.*;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.netty.util.AttributeKey;
import io.netty.util.AttributeMap;
import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import javax.inject.Inject;
import org.bson.BSONObject;


/**
 *
 */
public class ToroRequestProcessor extends AbstractRequestProcessor {
    public static final AtomicInteger connectionId = new AtomicInteger(0);
	
	public final static AttributeKey<ToroConnection> CONNECTION = 
			AttributeKey.valueOf("connection");
	public final static AttributeKey<LastError> LAST_ERROR = 
			AttributeKey.valueOf("lastError");
	
	private final Torod torod;

    @Inject
    public ToroRequestProcessor(Torod torod, 
    		QueryCommandProcessor queryCommandProcessor) {
        super(queryCommandProcessor);
        
        this.torod = torod;
    }

	@Override
	public void onChannelActive(AttributeMap attributeMap) {
		ToroConnection connection = torod.openConnection();
		attributeMap.attr(MessageReplier.CONNECTION_ID).set(connectionId.incrementAndGet());
        attributeMap.attr(CONNECTION).set(connection);
        attributeMap.attr(LAST_ERROR).set(new ToroLastError(
        		RequestOpCode.RESERVED, 
        		null, 
        		null, 
        		null, 
        		false, 
        		null));
	}

	@Override
	public void onChannelInactive(AttributeMap attributeMap) {
		ToroConnection connection = attributeMap.attr(CONNECTION).get();
		if (connection != null) {
			connection.close();
	        attributeMap.attr(CONNECTION).set(null);
		}
		attributeMap.attr(MessageReplier.CONNECTION_ID).set(null);
	}

	@Override
	@SuppressFBWarnings("NP_LOAD_OF_KNOWN_NULL_VALUE")
	public void queryNonCommand(QueryMessage queryMessage,
			MessageReplier messageReplier) throws Exception {
		AttributeMap attributeMap = messageReplier.getAttributeMap();
		ToroConnection connection = attributeMap.attr(CONNECTION).get();
		
        CursorManager cursorManager = connection.getCursorManager();
        
    	String collection = ToroCollectionTranslator.translate(queryMessage.getCollection());
    	QueryCriteriaTranslator queryCriteriaTranslator = new QueryCriteriaTranslator();
    	BSONDocument document = queryMessage.getDocument();
    	for (String key : document.getKeys()) {
    		if (QueryModifier.getByKey(key) != null || QuerySortOrder.getByKey(key) != null) {
    			throw new Exception("Modifier " + key + " not supported");
    		}
    	}
    	BSONObject query = ((MongoBSONDocument) document).getBSONObject();
    	for (String key : query.keySet()) {
    		if (QueryEncapsulation.getByKey(key) != null) {
    			Object queryObject = query.get(key);
    			if (queryObject != null && queryObject instanceof BSONObject) {
    				query = (BSONObject) queryObject;
    				break;
    			}
    		}
    	}
    	QueryCriteria queryCriteria = queryCriteriaTranslator.translate(query);
    	Projection projection = null;
    	int numberToSkip = queryMessage.getNumberToSkip();
    	int limit = queryMessage.getNumberToReturn();
    	boolean autoclose = false;
    	boolean hasTimeout = !queryMessage.isFlagSet(Flag.NO_CURSOR_TIMEOUT);
    	
    	CursorId cursorId;
    	BSONDocuments results;
    	
    	if (queryMessage.isFlagSet(Flag.TAILABLE_CURSOR)) {
    		messageReplier.replyQueryFailure(ErrorCode.UNIMPLEMENTED_FLAG, "TailableCursor");
    		return;
    	}
    	
    	if (limit < 0) {
    		autoclose = true;
    		limit = -limit;
    	} else if (limit == 1) {
    		autoclose = true;
    	}
    	
    	if (limit == 0) {
        	cursorId = cursorManager.openUnlimitedCursor(collection, queryCriteria, projection, numberToSkip, autoclose, hasTimeout);
    	} else {
        	cursorId = cursorManager.openLimitedCursor(collection, queryCriteria, projection, numberToSkip, limit, autoclose, hasTimeout);
    	}
        
   		results = new BSONDocuments(cursorManager.readCursor(cursorId, MongoWP.MONGO_CURSOR_LIMIT));

   		long cursorIdReturned = 0;
    	
    	if (results.size() >= MongoWP.MONGO_CURSOR_LIMIT) {
    		cursorIdReturned = cursorId.getNumericId();
    	} else {
    		cursorManager.closeCursor(cursorId);
    	}
    	
		messageReplier.replyMessageMultipleDocumentsWithFlags(cursorIdReturned, 0, results,
				EnumSet.noneOf(ReplyMessage.Flag.class));
	}

	@Override
	public void noSuchCommand(BSONDocument query, MessageReplier messageReplier) throws Exception {
		AttributeMap attributeMap = messageReplier.getAttributeMap();
    	MongoWP.ErrorCode errorCode = MongoWP.ErrorCode.NO_SUCH_COMMAND;
        LastError lastError = new ToroLastError(
        		RequestOpCode.OP_QUERY, 
        		null, 
        		null, 
        		null, 
        		true, 
        		errorCode);
        attributeMap.attr(ToroRequestProcessor.LAST_ERROR).set(lastError);
        messageReplier.replyQueryCommandFailure(
                errorCode, 
        		query.getKeys().iterator().hasNext()?query.getKeys().iterator().next():"");
	}

	@Override
	public void adminOnlyCommand(QueryCommand queryCommand, MessageReplier messageReplier)
			throws Exception {
		AttributeMap attributeMap = messageReplier.getAttributeMap();
    	MongoWP.ErrorCode errorCode = MongoWP.ErrorCode.MUST_RUN_ON_ADMIN;
        LastError lastError = new ToroLastError(
        		RequestOpCode.OP_QUERY, 
        		queryCommand, 
        		null, 
        		null, 
        		true, 
        		errorCode);
        attributeMap.attr(ToroRequestProcessor.LAST_ERROR).set(lastError);
        messageReplier.replyQueryCommandFailure(errorCode, queryCommand.getKey());
	}

	@Override
	public void getMore(GetMoreMessage getMoreMessage,
			MessageReplier messageReplier) throws Exception {
		AttributeMap attributeMap = messageReplier.getAttributeMap();
		
		CursorId cursorId = new CursorId(getMoreMessage.getCursorId());
		
        CursorManager cursorManager
                = attributeMap.attr(CONNECTION).get().getCursorManager();
        
		BSONDocuments results = 
				new BSONDocuments(
                        cursorManager.readCursor(
                                cursorId, 
                                MongoWP.MONGO_CURSOR_LIMIT
                        )
                );
    	
    	long cursorIdReturned = 0;
    	
    	cursorIdReturned = cursorId.getNumericId();
        Integer position = cursorManager.getPosition(cursorId).get();
		if (results.size() < MongoWP.MONGO_CURSOR_LIMIT) {
    		cursorManager.closeCursor(cursorId);
    	}
        
		messageReplier.replyMessageMultipleDocumentsWithFlags(
                cursorIdReturned, 
                position, 
                results,
				EnumSet.noneOf(ReplyMessage.Flag.class)
        );
	}

	@Override
	public void killCursors(
			KillCursorsMessage killCursorsMessage, MessageReplier messageReplier)
			throws Exception {
		AttributeMap attributeMap = messageReplier.getAttributeMap();
		ToroConnection connection = attributeMap.attr(CONNECTION).get();
		
		if (connection == null) {
			return;
		}
        CursorManager cursorManager = connection.getCursorManager();
		
		int numberOfCursors = killCursorsMessage.getNumberOfCursors();
		long[] cursorIds = killCursorsMessage.getCursorIds();
		for (int index = 0; index < numberOfCursors; index++) {
			CursorId cursorId = new CursorId(cursorIds[index]);
			
            cursorManager.closeCursor(cursorId);
		}
	}

	@Override
	public void insert(InsertMessage insertMessage,
			MessageReplier messageReplier) throws Exception {
		AttributeMap attributeMap = messageReplier.getAttributeMap();
        ToroConnection connection = attributeMap.attr(ToroRequestProcessor.CONNECTION).get();
		
		String collection = ToroCollectionTranslator.translate(insertMessage.getCollection());
    	WriteFailMode writeFailMode = 
    			insertMessage.getFlags().contains(InsertMessage.Flag.CONTINUE_ON_ERROR)?
    					WriteFailMode.ISOLATED:WriteFailMode.ORDERED;
		Iterable<?> documents = insertMessage.getDocuments();
    	List<ToroDocument> inserts = new ArrayList<ToroDocument>();
    	Iterator<?> documentsIterator = documents.iterator();
    	while(documentsIterator.hasNext()) {
    		BSONObject object = ((MongoBSONDocument) documentsIterator.next()).getBSONObject();
    		inserts.add(new BSONToroDocument(object));
    	}
		
        ToroTransaction transaction = connection.createTransaction();
		
        try {
        	Future<InsertResponse> futureInsertResponse = transaction.insertDocuments(collection, inserts, writeFailMode);
        	
            Future<?> futureCommitResponse = transaction.commit();
            
            LastError lastError = new ToroLastError(
            		RequestOpCode.OP_INSERT, 
            		null, 
            		futureInsertResponse, 
            		futureCommitResponse, 
            		false, 
            		null);
            attributeMap.attr(ToroRequestProcessor.LAST_ERROR).set(lastError);
        } finally {
           	transaction.close();
        }
	}

	@Override
	public void update(UpdateMessage updateMessage,
			MessageReplier messageReplier) throws Exception {
		AttributeMap attributeMap = messageReplier.getAttributeMap();
		ToroConnection connection = attributeMap.attr(CONNECTION).get();
		
    	String collection = ToroCollectionTranslator.translate(updateMessage.getCollection());
    	WriteFailMode writeFailMode = WriteFailMode.ORDERED;
    	QueryCriteriaTranslator queryCriteriaTranslator = new QueryCriteriaTranslator();
    	BSONDocument selector = updateMessage.getSelector();
    	for (String key : selector.getKeys()) {
    		if (QueryModifier.getByKey(key) != null || QuerySortOrder.getByKey(key) != null) {
    			throw new Exception("Modifier " + key + " not supported");
    		}
    	}
    	BSONObject query = ((MongoBSONDocument) selector).getBSONObject();
    	for (String key : query.keySet()) {
    		if (QueryEncapsulation.getByKey(key) != null) {
    			Object queryObject = query.get(key);
    			if (queryObject != null && queryObject instanceof BSONObject) {
    				query = (BSONObject) queryObject;
    				break;
    			}
    		}
    	}
    	QueryCriteria queryCriteria = queryCriteriaTranslator.translate(query);
    	List<UpdateOperation> updates = new ArrayList<UpdateOperation>();
    	boolean upsert = updateMessage.isFlagSet(UpdateMessage.Flag.UPSERT);
    	boolean justOne = !updateMessage.isFlagSet(UpdateMessage.Flag.MULTI_UPDATE);
    	
    	UpdateAction updateAction = UpdateActionTranslator.translate(
    			((MongoBSONDocument)updateMessage.getupdate()).getBSONObject()); 

    	updates.add(new UpdateOperation(queryCriteria, updateAction, upsert, justOne));

    	ToroTransaction transaction = connection.createTransaction();
        
        try {
	       	Future<UpdateResponse> futureUpdateResponse = transaction.update(collection, updates, writeFailMode);
        	
            Future<?> futureCommitResponse = transaction.commit();
            
            LastError lastError = new ToroLastError(
            		RequestOpCode.OP_UPDATE, 
            		null, 
            		futureUpdateResponse, 
            		futureCommitResponse, 
            		false, 
            		null);
            attributeMap.attr(ToroRequestProcessor.LAST_ERROR).set(lastError);
		} finally {
           	transaction.close();
    	}
	}

	@Override
	public void delete(DeleteMessage deleteMessage,
			MessageReplier messageReplier) throws Exception {
		AttributeMap attributeMap = messageReplier.getAttributeMap();
		ToroConnection connection = attributeMap.attr(CONNECTION).get();
		
    	String collection = ToroCollectionTranslator.translate(deleteMessage.getCollection());
    	WriteFailMode writeFailMode = WriteFailMode.ORDERED;
    	QueryCriteriaTranslator queryCriteriaTranslator = new QueryCriteriaTranslator();
    	BSONDocument document = deleteMessage.getDocument();
    	for (String key : document.getKeys()) {
    		if (QueryModifier.getByKey(key) != null || QuerySortOrder.getByKey(key) != null) {
    			throw new Exception("Modifier " + key + " not supported");
    		}
    	}
    	BSONObject query = ((MongoBSONDocument) document).getBSONObject();
    	for (String key : query.keySet()) {
    		if (QueryEncapsulation.getByKey(key) != null) {
    			Object queryObject = query.get(key);
    			if (queryObject != null && queryObject instanceof BSONObject) {
    				query = (BSONObject) queryObject;
    				break;
    			}
    		}
    	}
    	QueryCriteria queryCriteria = queryCriteriaTranslator.translate(query);
    	List<DeleteOperation> deletes = new ArrayList<DeleteOperation>();
    	boolean singleRemove = deleteMessage.isFlagSet(DeleteMessage.Flag.SINGLE_REMOVE);

    	deletes.add(new DeleteOperation(queryCriteria, singleRemove));

    	ToroTransaction transaction = connection.createTransaction();
        
        try {
	       	Future<DeleteResponse> futureDeleteResponse = transaction.delete(collection, deletes, writeFailMode);
        	
            Future<?> futureCommitResponse = transaction.commit();
            
            LastError lastError = new ToroLastError(
            		RequestOpCode.OP_DELETE, 
            		null, 
            		futureDeleteResponse, 
            		futureCommitResponse, 
            		false, 
            		null);
            attributeMap.attr(ToroRequestProcessor.LAST_ERROR).set(lastError);
		} finally {
           	transaction.close();
    	}
	}

	@Override
	public boolean handleError(RequestOpCode requestOpCode, MessageReplier messageReplier, Throwable throwable)
			throws Exception {
		if (super.handleError(requestOpCode, messageReplier, throwable)) {
			return true;
		}
		
		//TODO: Map real mongo error codes		
		AttributeMap attributeMap = messageReplier.getAttributeMap();
    	MongoWP.ErrorCode errorCode = MongoWP.ErrorCode.INTERNAL_ERROR;
        LastError lastError = new ToroLastError(
        		requestOpCode, 
        		null, 
        		null, 
        		null, 
        		true, 
        		errorCode);
        attributeMap.attr(ToroRequestProcessor.LAST_ERROR).set(lastError);
        switch(requestOpCode) {
        case OP_QUERY:
        	messageReplier.replyQueryFailure(errorCode, throwable.getMessage());
        	break;
        default:
        	break;
        }
        
		return true;
	}

}
