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
import com.eightkdata.mongowp.messages.response.ReplyMessage;
import com.eightkdata.mongowp.mongoserver.api.AbstractRequestProcessor;
import com.eightkdata.mongowp.mongoserver.api.MetaCommandProcessor;
import com.eightkdata.mongowp.mongoserver.api.QueryCommandProcessor;
import com.eightkdata.mongowp.mongoserver.api.QueryCommandProcessor.QueryCommand;
import com.eightkdata.mongowp.mongoserver.api.callback.LastError;
import com.eightkdata.mongowp.mongoserver.api.callback.MessageReplier;
import com.eightkdata.mongowp.mongoserver.api.commands.QueryReply;
import com.eightkdata.mongowp.mongoserver.api.commands.QueryRequest;
import com.eightkdata.mongowp.mongoserver.protocol.MongoWP;
import com.eightkdata.nettybson.api.BSONDocument;
import com.eightkdata.nettybson.mongodriver.MongoBSONDocument;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.torodb.mongowp.mongoserver.api.toro.util.BSONDocuments;
import com.torodb.mongowp.mongoserver.api.toro.util.BSONToroDocument;
import com.torodb.torod.core.Torod;
import com.torodb.torod.core.WriteFailMode;
import com.torodb.torod.core.connection.*;
import com.torodb.torod.core.cursors.CursorId;
import com.torodb.torod.core.cursors.UserCursor;
import com.torodb.torod.core.exceptions.CursorNotFoundException;
import com.torodb.torod.core.exceptions.UserToroException;
import com.torodb.torod.core.language.operations.DeleteOperation;
import com.torodb.torod.core.language.operations.UpdateOperation;
import com.torodb.torod.core.language.projection.Projection;
import com.torodb.torod.core.language.querycriteria.QueryCriteria;
import com.torodb.torod.core.language.querycriteria.TrueQueryCriteria;
import com.torodb.torod.core.language.update.UpdateAction;
import com.torodb.torod.core.subdocument.ToroDocument;
import com.torodb.translator.*;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.netty.util.AttributeKey;
import io.netty.util.AttributeMap;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import javax.inject.Inject;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.types.BasicBSONList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *
 */
public class ToroRequestProcessor extends AbstractRequestProcessor {
    private static final Logger LOGGER
            = LoggerFactory.getLogger(ToroRequestProcessor.class);
    public static final AtomicInteger connectionId = new AtomicInteger(0);
	
	public final static AttributeKey<ToroConnection> CONNECTION = 
			AttributeKey.valueOf("connection");
	public final static AttributeKey<LastError> LAST_ERROR = 
			AttributeKey.valueOf("lastError");
	
	private final Torod torod;
    private final QueryCriteriaTranslator queryCriteriaTranslator;
    private final ToroMetaCommandProcessor metaProcessor;

    @Inject
    public ToroRequestProcessor(
            Torod torod, 
            QueryCommandProcessor queryCommandProcessor, 
            MetaCommandProcessor metaQueryProcessor,
            QueryCriteriaTranslator queryCriteriaTranslator,
            ToroMetaCommandProcessor metaProcessor) {
        super(queryCommandProcessor, metaQueryProcessor);
        this.torod = torod;
        this.queryCriteriaTranslator = queryCriteriaTranslator;
        this.metaProcessor = metaProcessor;
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
	public QueryReply query(QueryRequest request) throws Exception {
		AttributeMap attributeMap = request.getAttributes();
		ToroConnection connection = attributeMap.attr(CONNECTION).get();
		
        if (request.getProjection() != null) {
            throw new UserToroException("Projections are not supported");
        }
    	QueryCriteria queryCriteria;
        if (request.getQuery() == null) {
            queryCriteria = TrueQueryCriteria.getInstance();
        } else {
            queryCriteria = queryCriteriaTranslator.translate(
                request.getQuery()
            );
        }
    	Projection projection = null;
    	
    	UserCursor<ToroDocument> cursor;
    	BSONDocuments results;
    	
        if (request.isTailable()) {
            throw new UserToroException("TailableCursors are not supported");
        }
        
    	if (request.getLimit() == 0) {
            cursor = connection.openUnlimitedCursor(
                    request.getCollection(), 
                    queryCriteria, 
                    projection, 
                    request.getNumberToSkip(),
                    request.isAutoclose(),
                    !request.isNoCursorTimeout()
            );
    	} else {
            cursor = connection.openLimitedCursor(
                    request.getCollection(),
                    queryCriteria,
                    projection,
                    request.getNumberToSkip(),
                    request.getLimit(),
                    request.isAutoclose(),
                    !request.isNoCursorTimeout()
            );
    	}
        
   		results = new BSONDocuments(cursor.read(MongoWP.MONGO_CURSOR_LIMIT));

   		long cursorIdReturned = 0;
    	
    	if (results.size() >= MongoWP.MONGO_CURSOR_LIMIT) {
    		cursorIdReturned = cursor.getId().getNumericId();
    	} else {
    		cursor.close();
    	}
        
        return new QueryReply.Builder()
                .setCursorId(cursorIdReturned)
                .setStartingFrom(0)
                .setDocuments(results)
                .build();
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
		ToroConnection connection = attributeMap.attr(CONNECTION).get();
        
		CursorId cursorId = new CursorId(getMoreMessage.getCursorId());
		
        try {
            UserCursor cursor = connection.getCursor(cursorId);

            BSONDocuments results = 
                    new BSONDocuments(
                            cursor.read(MongoWP.MONGO_CURSOR_LIMIT)
                    );

            boolean cursorEmptied = results.size() < MongoWP.MONGO_CURSOR_LIMIT;

            Integer position = cursor.getPosition();
            if (cursorEmptied) {
                cursor.close();
            }

            messageReplier.replyMessageMultipleDocumentsWithFlags(
                    cursorEmptied ? 0 : cursorId.getNumericId(),        // Signal "don't read more from cursor" if emptied
                    position, 
                    results,
                    EnumSet.noneOf(ReplyMessage.Flag.class)
            );
        } catch (CursorNotFoundException ex) {
            throw ex; //TODO: change that to translate the exception to whatever mongo protocol used to notify a closed or no existing cursor
        }
        
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
        
		int numberOfCursors = killCursorsMessage.getNumberOfCursors();
		long[] cursorIds = killCursorsMessage.getCursorIds();
		for (int index = 0; index < numberOfCursors; index++) {
			CursorId cursorId = new CursorId(cursorIds[index]);
			
            try {
                connection.getCursor(cursorId).close();
            } catch (CursorNotFoundException ex) {
            }
		}
	}

	@Override
	public void insert(InsertMessage insertMessage,
			MessageReplier messageReplier) throws Exception {
		AttributeMap attributeMap = messageReplier.getAttributeMap();
        ToroConnection connection = attributeMap.attr(ToroRequestProcessor.CONNECTION).get();
		
		String collection = insertMessage.getCollection();
        
        if (metaProcessor.isMetaCollection(collection)) {
            BSONObject insertQuery = new BasicBSONObject();
            insertQuery.put("insert", collection);
            insertQuery.put("ordered", insertMessage.isFlagSet(InsertMessage.Flag.CONTINUE_ON_ERROR));
            
            BasicBSONList docsToInsert = new BasicBSONList();
            for (BSONDocument document : insertMessage.getDocuments()) {
                docsToInsert.add(new BasicBSONObject(document.asMap()));
            }
            insertQuery.put("documents", docsToInsert);
            
            Future<InsertResponse> future;
            try {
                Future<com.eightkdata.mongowp.mongoserver.api.pojos.InsertResponse> response
                        = metaProcessor.insert(messageReplier.getAttributeMap(), collection, new MongoBSONDocument(insertQuery));
                future = Futures.lazyTransform(response, new InsertResponseTransformFunction());
            } catch (Exception ex) {
                future = Futures.immediateFailedFuture(ex);
            }
            
            LastError lastError = new ToroLastError(RequestOpCode.OP_INSERT, null, future, null, false, null);
            attributeMap.attr(ToroRequestProcessor.LAST_ERROR).set(lastError);
        }
        else {
            WriteFailMode writeFailMode = 
    //    			insertMessage.getFlags().contains(InsertMessage.Flag.CONTINUE_ON_ERROR)?
    //    					WriteFailMode.ISOLATED:WriteFailMode.ORDERED;
                    WriteFailMode.TRANSACTIONAL;
            List<BSONDocument> documents = insertMessage.getDocuments();
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
	}

	@Override
	public void update(UpdateMessage updateMessage,
			MessageReplier messageReplier) throws Exception {
		AttributeMap attributeMap = messageReplier.getAttributeMap();
		ToroConnection connection = attributeMap.attr(CONNECTION).get();
		
    	String collection = updateMessage.getCollection();
    	WriteFailMode writeFailMode = WriteFailMode.ORDERED;
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
		
    	String collection = deleteMessage.getCollection();
    	WriteFailMode writeFailMode = WriteFailMode.ORDERED;
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

    private static class InsertResponseTransformFunction implements Function<com.eightkdata.mongowp.mongoserver.api.pojos.InsertResponse, InsertResponse> {

        @Override
        public InsertResponse apply(com.eightkdata.mongowp.mongoserver.api.pojos.InsertResponse input) {
            if (input == null) {
                return null;
            }
            List<WriteError> errors = Lists.newArrayListWithCapacity(input.getWriteErrors().size());
            for (com.eightkdata.mongowp.mongoserver.api.pojos.InsertResponse.WriteError writeError : input.getWriteErrors()) {
                errors.add(new WriteError(writeError.getIndex(), writeError.getCode(), writeError.getErrmsg()));
            }
            return new InsertResponse(input.isOk(), input.getN(), errors);
        }
    }

}
