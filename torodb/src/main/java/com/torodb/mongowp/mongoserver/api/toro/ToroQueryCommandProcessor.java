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

import com.eightkdata.mongowp.mongoserver.api.commands.QueryAndWriteOperationsQueryCommand;
import com.eightkdata.mongowp.mongoserver.api.commands.pojos.CollStatsRequest;
import com.eightkdata.mongowp.mongoserver.api.commands.pojos.CollStatsReply;
import com.eightkdata.mongowp.mongoserver.api.commands.pojos.CountReply;
import com.eightkdata.mongowp.mongoserver.api.commands.pojos.CountRequest;
import com.eightkdata.mongowp.messages.request.RequestOpCode;
import com.eightkdata.mongowp.mongoserver.api.QueryCommandProcessor;
import com.eightkdata.mongowp.mongoserver.api.callback.LastError;
import com.eightkdata.mongowp.mongoserver.api.callback.MessageReplier;
import com.eightkdata.mongowp.mongoserver.protocol.MongoWP;
import com.eightkdata.mongowp.mongoserver.protocol.MongoWP.ErrorCode;
import com.eightkdata.nettybson.api.BSONDocument;
import com.eightkdata.nettybson.mongodriver.MongoBSONDocument;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.mongodb.WriteConcern;
import com.torodb.BuildProperties;
import com.torodb.kvdocument.conversion.mongo.MongoValueConverter;
import com.torodb.kvdocument.values.DocValue;
import com.torodb.kvdocument.values.ObjectValue;
import com.torodb.mongowp.mongoserver.api.toro.util.BSONToroDocument;
import com.torodb.torod.core.WriteFailMode;
import com.torodb.torod.core.annotations.DatabaseName;
import com.torodb.torod.core.connection.*;
import com.torodb.torod.core.cursors.UserCursor;
import com.torodb.torod.core.dbWrapper.exceptions.ImplementationDbException;
import com.torodb.torod.core.exceptions.ExistentIndexException;
import com.torodb.torod.core.exceptions.UserToroException;
import com.torodb.torod.core.language.AttributeReference;
import com.torodb.torod.core.language.operations.DeleteOperation;
import com.torodb.torod.core.language.operations.UpdateOperation;
import com.torodb.torod.core.language.querycriteria.QueryCriteria;
import com.torodb.torod.core.language.querycriteria.TrueQueryCriteria;
import com.torodb.torod.core.language.update.UpdateAction;
import com.torodb.torod.core.pojos.CollectionMetainfo;
import com.torodb.torod.core.pojos.Database;
import com.torodb.torod.core.pojos.IndexedAttributes;
import com.torodb.torod.core.pojos.NamedToroIndex;
import com.torodb.torod.core.subdocument.ToroDocument;
import com.torodb.torod.core.utils.DocValueQueryCriteriaEvaluator;
import com.torodb.translator.*;
import io.netty.util.AttributeMap;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.bson.types.BasicBSONList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class ToroQueryCommandProcessor implements QueryCommandProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(ToroQueryCommandProcessor.class);
    private final QueryCriteriaTranslator queryCriteriaTranslator;
	private final BuildProperties buildProperties;
    private final String databaseName;

	@Inject
	public ToroQueryCommandProcessor(
            BuildProperties buildProperties,
            QueryCriteriaTranslator queryCriteriaTranslator,
            @DatabaseName String databaseName) {
		this.buildProperties = buildProperties;
        this.queryCriteriaTranslator = queryCriteriaTranslator;
        this.databaseName = databaseName;
	}

    @Override
    public CountReply count(CountRequest request) throws Exception {
        AttributeMap attributeMap = request.getAttributes();
        ToroConnection connection = attributeMap.attr(ToroRequestProcessor.CONNECTION).get();

    	QueryCriteria queryCriteria;
        if (request.getQuery() == null) {
            queryCriteria = TrueQueryCriteria.getInstance();
        } else {
            queryCriteria = queryCriteriaTranslator.translate(
                request.getQuery()
            );
        }
        
        ToroTransaction transaction = connection.createTransaction();
        
        try {
            Integer count = transaction.count(
                    request.getCollection(), 
                    queryCriteria
            ).get();
            
            return new CountReply(count);
        } finally {
            transaction.close();
        }
	}

	@Override
	public com.eightkdata.mongowp.mongoserver.api.commands.pojos.InsertReply insert(
            BSONDocument document, AttributeMap attributeMap) throws Exception {
        ToroConnection connection = attributeMap.attr(ToroRequestProcessor.CONNECTION).get();
		
		String collection = (String) document.getValue("insert");
    	WriteFailMode writeFailMode = getWriteFailMode(document);
    	WriteConcern writeConcern = getWriteConcern(document);
		Iterable<?> documents = (Iterable<?>) document.getValue("documents");
    	List<ToroDocument> inserts = new ArrayList<ToroDocument>();
    	Iterator<?> documentsIterator = documents.iterator();
    	while(documentsIterator.hasNext()) {
    		BSONObject object = (BSONObject) documentsIterator.next();
    		inserts.add(new BSONToroDocument(object));
    	}
		
        ToroTransaction transaction = connection.createTransaction();
        
        ToroInsertReply.Builder responseBuilder = new ToroInsertReply.Builder();
		
        try {
        	Future<InsertResponse> futureInsertResponse = transaction.insertDocuments(collection, inserts, writeFailMode);
        	
            Future<?> futureCommitResponse = transaction.commit();
            
            if (writeConcern.getW() > 0) {
	            InsertResponse insertResponse = futureInsertResponse.get();
	            futureCommitResponse.get();
                responseBuilder.setN(insertResponse.getInsertedSize());
            }
            LastError lastError = new ToroLastError(
            		RequestOpCode.OP_QUERY, 
            		QueryAndWriteOperationsQueryCommand.delete, 
            		futureInsertResponse, 
            		futureCommitResponse, 
            		false, 
            		null);
            attributeMap.attr(ToroRequestProcessor.LAST_ERROR).set(lastError);
            
            responseBuilder.setOk(true);
            return responseBuilder.build();
        } finally {
           	transaction.close();
        }
	}

	@Override
	public void update(BSONDocument document, MessageReplier messageReplier) throws Exception {
		AttributeMap attributeMap = messageReplier.getAttributeMap();
        ToroConnection connection = attributeMap.attr(ToroRequestProcessor.CONNECTION).get();
		
        Map<String, Object> keyValues = new HashMap<String, Object>();
		
		String collection = (String) document.getValue("update");
    	
    	WriteFailMode writeFailMode = getWriteFailMode(document);
    	WriteConcern writeConcern = getWriteConcern(document);
		Iterable<?> documents = (Iterable<?>) document.getValue("updates");
    	List<UpdateOperation> updates = new ArrayList<UpdateOperation>();
    	Iterator<?> documentsIterator = documents.iterator();
    	while(documentsIterator.hasNext()) {
    		BSONObject object = (BSONObject) documentsIterator.next();
    		QueryCriteria queryCriteria = queryCriteriaTranslator.translate(
    				(BSONObject) object.get("q"));
    		UpdateAction updateAction = UpdateActionTranslator.translate(
    				(BSONObject) object.get("u"));
    		boolean upsert = getBoolean(object, "upsert", false);
    		boolean onlyOne = !getBoolean(object, "multi", false);
    		updates.add(new UpdateOperation(queryCriteria, updateAction, upsert, onlyOne));
    	}
		
        ToroTransaction transaction = connection.createTransaction();
        
        try {
        	Future<UpdateResponse> futureUpdateResponse = transaction.update(collection, updates, writeFailMode);
        	
            Future<?> futureCommitResponse = transaction.commit();
            
            if (writeConcern.getW() > 0) {
	            UpdateResponse updateResponse = futureUpdateResponse.get();
	            futureCommitResponse.get();
				keyValues.put("n", updateResponse.getModified());
            }
            LastError lastError = new ToroLastError(
            		RequestOpCode.OP_QUERY, 
            		QueryAndWriteOperationsQueryCommand.update, 
            		futureUpdateResponse, 
            		futureCommitResponse, 
            		false, 
            		null);
            attributeMap.attr(ToroRequestProcessor.LAST_ERROR).set(lastError);
        } finally {
           	transaction.close();
        }
		
		keyValues.put("ok", MongoWP.OK);
		BSONDocument reply = new MongoBSONDocument(keyValues);
		messageReplier.replyMessageNoCursor(reply);
	}

	private boolean getBoolean(BSONDocument document, String key, boolean defaultValue) {
		if (!document.hasKey(key)) {
			return defaultValue;
		}
		
		Object value = document.getValue(key);
		
		if (value instanceof Boolean) {
			return ((Boolean) value);
		}
		
		if (value instanceof Number) {
			return ((Number) value).intValue() > 0;
		}
		
		throw new IllegalArgumentException("Value " + value + " for key " + key + " is not boolean");
	}

	private boolean getBoolean(BSONObject object, String key, boolean defaultValue) {
		if (!object.containsField(key)) {
			return defaultValue;
		}
		
		Object value = object.get(key);
		
		if (value instanceof Boolean) {
			return ((Boolean) value);
		}
		
		if (value instanceof Number) {
			return ((Number) value).intValue() > 0;
		}
		
		throw new IllegalArgumentException("Value " + value + " for key " + key + " is not boolean");
	}

	@Override
	public void delete(BSONDocument document, MessageReplier messageReplier) throws Exception {
		AttributeMap attributeMap = messageReplier.getAttributeMap();
        ToroConnection connection = attributeMap.attr(ToroRequestProcessor.CONNECTION).get();
		
        Map<String, Object> keyValues = new HashMap<String, Object>();
		
		String collection = (String) document.getValue("delete");
    	
    	WriteFailMode writeFailMode = getWriteFailMode(document);
    	WriteConcern writeConcern = getWriteConcern(document);
		Iterable<?> documents = (Iterable<?>) document.getValue("deletes");
    	List<DeleteOperation> deletes = new ArrayList<DeleteOperation>();
    	Iterator<?> documentsIterator = documents.iterator();
    	while(documentsIterator.hasNext()) {
    		BSONObject object = (BSONObject) documentsIterator.next();
    		QueryCriteria queryCriteria = queryCriteriaTranslator.translate(
    				(BSONObject) object.get("q"));
    		boolean singleRemove = getBoolean(object, "limit", false);
    		deletes.add(new DeleteOperation(queryCriteria, singleRemove));
    	}
		
        ToroTransaction transaction = connection.createTransaction();
        
        try {
        	Future<DeleteResponse> futureDeleteResponse = transaction.delete(collection, deletes, writeFailMode);
        	
            Future<?> futureCommitResponse = transaction.commit();
            
            if (writeConcern.getW() > 0) {
	            DeleteResponse deleteResponse = futureDeleteResponse.get();
	            futureCommitResponse.get();
				keyValues.put("n", deleteResponse.getDeleted());
            }
            LastError lastError = new ToroLastError(
            		RequestOpCode.OP_QUERY, 
            		QueryAndWriteOperationsQueryCommand.delete, 
            		futureDeleteResponse, 
            		futureCommitResponse, 
            		false, 
            		null);
            attributeMap.attr(ToroRequestProcessor.LAST_ERROR).set(lastError);
        } finally {
           	transaction.close();
        }
		
		keyValues.put("ok", MongoWP.OK);
		BSONDocument reply = new MongoBSONDocument(keyValues);
		messageReplier.replyMessageNoCursor(reply);
	}

	@Override
	public void drop(BSONDocument document, MessageReplier messageReplier) throws Exception {
		AttributeMap attributeMap = messageReplier.getAttributeMap();
        ToroConnection connection = attributeMap.attr(ToroRequestProcessor.CONNECTION).get();
		
        Map<String, Object> keyValues = new HashMap<String, Object>();
		
		String collection = (String) document.getValue("drop");

        connection.dropCollection(collection);
        
        keyValues.put("ok", MongoWP.OK);
        BSONDocument reply = new MongoBSONDocument(keyValues);
        messageReplier.replyMessageNoCursor(reply);

	}
    
    private AttributeReference parseAttributeReference(String path) {
        AttributeReference.Builder builder = new AttributeReference.Builder();
        
        StringTokenizer st = new StringTokenizer(path, ".");
        while (st.hasMoreTokens()) {
            builder.addObjectKey(st.nextToken());
        }
        return builder.build();
    }
	
	//TODO: implement with toro natives
	@Override
    public void createIndexes(@Nonnull BSONDocument document, @Nonnull MessageReplier messageReplier)
            throws Exception {
        BSONObject keyValues = new BasicBSONObject();

        Object collectionValue = document.getValue("createIndexes");
        if (!(collectionValue instanceof String)) {
            keyValues.put("ok", MongoWP.KO);
            keyValues.put("code", (double) 13111);
            keyValues.put("errmsg", "exception: wrong type for field (createIndexes)");
            messageReplier.replyMessageNoCursor(new MongoBSONDocument(keyValues));
            return;
        }
        String collection = (String) collectionValue;
        Object indexesValue = document.getValue("indexes");
        if (!(indexesValue instanceof List)) {
            keyValues.put("ok", MongoWP.KO);
            keyValues.put("errmsg", "indexes has to be an array");
            messageReplier.replyMessageNoCursor(new MongoBSONDocument(keyValues));
            return;
        }
        //TODO: Unsafe cast
        List<BSONObject> uncastedIndexes = (List<BSONObject>) indexesValue;

        AttributeMap attributeMap = messageReplier.getAttributeMap();
        ToroConnection connection
                = attributeMap.attr(ToroRequestProcessor.CONNECTION).get();

        int numIndexesBefore;

        ToroTransaction transaction = null;
        try {
            transaction = connection.createTransaction();

            numIndexesBefore = transaction.getIndexes(collection).size();
            try {
                for (BSONObject uncastedIndex : uncastedIndexes) {
                    String name = (String) uncastedIndex.removeField("name");
                    BSONObject key
                            = (BSONObject) uncastedIndex.removeField("key");
                    Boolean unique
                            = (Boolean) uncastedIndex.removeField("unique");
                    unique = unique != null ? unique : false;
                    Boolean sparse
                            = (Boolean) uncastedIndex.removeField("sparse");
                    sparse = sparse != null ? sparse : false;

                    if (!uncastedIndex.keySet().isEmpty()) {
                        keyValues.put("ok", MongoWP.KO);
                        String errmsg = "Options "
                                + uncastedIndex.keySet().toString()
                                + " are not supported";
                        keyValues.put("errmsg", errmsg);
                        messageReplier.replyMessageNoCursor(new MongoBSONDocument(keyValues));
                        return;
                    }

                    IndexedAttributes.Builder indexedAttsBuilder
                            = new IndexedAttributes.Builder();

                    for (String path : key.keySet()) {
                        AttributeReference attRef
                                = parseAttributeReference(path);
                        //TODO: Check that key.get(path) is a number!!
                        boolean ascending = ((Number) key.get(path)).intValue()
                                > 0;

                        indexedAttsBuilder.addAttribute(attRef, ascending);
                    }

                    transaction.createIndex(collection, name, indexedAttsBuilder.build(), unique, sparse).get();
                }

                int numIndexesAfter = transaction.getIndexes(collection).size();

                transaction.commit().get();

                keyValues.put("ok", MongoWP.OK);
                keyValues.put("createdCollectionAutomatically", false);
                keyValues.put("numIndexesBefore", numIndexesBefore);
                keyValues.put("numIndexesAfter", numIndexesAfter);

                BSONDocument reply = new MongoBSONDocument(keyValues);
                messageReplier.replyMessageNoCursor(reply);
            }
            catch (ExecutionException ex) {
                if (ex.getCause() instanceof ExistentIndexException) {
                    keyValues.put("ok", MongoWP.OK);
                    keyValues.put("note", ex.getCause().getMessage());
                    keyValues.put("numIndexesBefore", numIndexesBefore);

                    messageReplier.replyMessageNoCursor(new MongoBSONDocument(keyValues));
                }
                else {
                    throw ex;
                }
            }
        }
        finally {
            if (transaction != null) {
                transaction.close();
            }
        }

    }

    @Override
    public void deleteIndexes(BSONDocument query, MessageReplier messageReplier) throws Exception {
        Map<String, Object> keyValues = Maps.newHashMap();
        
        Object dropIndexesValue = query.getValue("deleteIndexes");
        Object indexValue = query.getValue("index");
        
        if (!(dropIndexesValue instanceof String)) {
            keyValues.put("ok", MongoWP.KO);
            keyValues.put("errmsg", "The field 'dropIndexes' must be a string");
            messageReplier.replyMessageNoCursor(new MongoBSONDocument(keyValues));
            return ;
        }
        if (!(indexValue instanceof String)) {
            keyValues.put("ok", MongoWP.KO);
            keyValues.put("errmsg", "The field 'index' must be a string");
            messageReplier.replyMessageNoCursor(new MongoBSONDocument(keyValues));
            return ;
        }
        
        String collection = (String) dropIndexesValue;
        String indexName = (String) indexValue;
        
        AttributeMap attributeMap = messageReplier.getAttributeMap();
        ToroConnection connection = attributeMap.attr(ToroRequestProcessor.CONNECTION).get();
        
        ToroTransaction transaction = null;
        try {
            transaction = connection.createTransaction();
            
            if (indexName.equals("*")) { //TODO: Support * in deleteIndexes
                keyValues.put("ok", MongoWP.KO);
                keyValues.put("errmsg", "The wildcard '*' is not supported by ToroDB right now");
                messageReplier.replyMessageNoCursor(new MongoBSONDocument(keyValues));
                return ;
            }
        
            Boolean removed = transaction.dropIndex(collection, indexName).get();
            if (!removed) {
                keyValues.put("ok", MongoWP.KO);
                keyValues.put("errmsg", "index not found with name ["+indexName+"]");
                messageReplier.replyMessageNoCursor(new MongoBSONDocument(keyValues));
                return ;
            }
            
            transaction.commit();
            
            keyValues.put("ok", MongoWP.OK);
            messageReplier.replyMessageNoCursor(new MongoBSONDocument(keyValues));
        } finally {
            if (transaction != null) {
                transaction.close();
            }
        }
    }
	
	@Override
	public void create(BSONDocument document, MessageReplier messageReplier)
			throws Exception {
		String collection = (String) document.getValue("create");
		Boolean capped = (Boolean) document.getValue("capped");

		if (capped != null && capped) { // Other flags silently ignored
			messageReplier.replyQueryCommandFailure(ErrorCode.UNIMPLEMENTED_FLAG, "capped");
			return;
		}

		AttributeMap attributeMap = messageReplier.getAttributeMap();
		ToroConnection connection = attributeMap.attr(
				ToroRequestProcessor.CONNECTION).get();

		Map<String, Object> keyValues = new HashMap<String, Object>();
		if(connection.createCollection(collection, null)) {
			keyValues.put("ok", MongoWP.OK);
		} else {
			keyValues.put("ok", MongoWP.KO);
			keyValues.put("errmsg", "collection already exists");
		}

		messageReplier.replyMessageNoCursor(new MongoBSONDocument(keyValues));
	}

	private WriteFailMode getWriteFailMode(BSONDocument document) {
        return WriteFailMode.TRANSACTIONAL;
	}

	private WriteConcern getWriteConcern(BSONDocument document) {
		WriteConcern writeConcern = WriteConcern.ACKNOWLEDGED;
    	if (document.hasKey("writeConcern")) {
	    	BSONObject writeConcernObject = (BSONObject) document.getValue("writeConcern");
	    	Object w = writeConcernObject.get("w");
	        int wtimeout = 0;
	        boolean fsync = false;
	        boolean j = false;
	        boolean continueOnError = false;
	        Object jObject = writeConcernObject.get("j");
	        if (jObject !=null && jObject instanceof Boolean && 
	        		(Boolean)jObject) {
	        	fsync = true;
	        	j = true;
	        	continueOnError = true;
	        }
	        Object wtimeoutObject = writeConcernObject.get("wtimneout");
	        if (wtimeoutObject !=null && wtimeoutObject instanceof Number) {
	        	wtimeout = ((Number)wtimeoutObject).intValue();
	        }
	    	if (w != null) {
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
	    	}
    	}
		return writeConcern;
	}

	@Override
	public void getLastError(Object w, boolean j, 
			boolean fsync, int wtimeout, MessageReplier messageReplier) throws Exception {
		AttributeMap attributeMap = messageReplier.getAttributeMap();
		LastError lastError = attributeMap.attr(ToroRequestProcessor.LAST_ERROR).get();
		lastError.getLastError(w, j, fsync, wtimeout, messageReplier);
	}

	@Override
	public void validate(String database, BSONDocument document, MessageReplier messageReplier) {
		Map<String, Object> keyValues = new HashMap<String, Object>();
		
		String collection = (String) document.getValue("validate");
		boolean full = getBoolean(document, "full", false);
		String ns = database + "." + collection;
		
		keyValues.put("ns", ns);
		keyValues.put("firstExtent", "2:4b4b000 ns:" + ns);
		keyValues.put("lastExtent", "2:4b4b000 ns:" + ns);
		keyValues.put("extentCount", 1);
		keyValues.put("datasize", 0);
		keyValues.put("nrecords", 0);
		keyValues.put("lastExtentSize", 8192);
		keyValues.put("padding", 0);
		
		Map<String, Object> firstExtentDetailsKeyValues = new HashMap<String, Object>();
		firstExtentDetailsKeyValues.put("loc", "2:4b4b000");
		firstExtentDetailsKeyValues.put("xnext", null);
		firstExtentDetailsKeyValues.put("xprev", null);
		firstExtentDetailsKeyValues.put("nsdiag", ns);
		firstExtentDetailsKeyValues.put("size", 8192);
		firstExtentDetailsKeyValues.put("firstRecord", null);
		firstExtentDetailsKeyValues.put("firstRecord", null);
		keyValues.put("firstExtentDetails", new BasicBSONObject(firstExtentDetailsKeyValues));
		
		keyValues.put("deletedCount", 0);
		keyValues.put("deletedSize", 0);
		keyValues.put("nIndexes", 1);

		Map<String, Object> keysPerIndexKeyValues = new HashMap<String, Object>();
		keysPerIndexKeyValues.put(ns + ".$_id_", 0);
		keyValues.put("keysPerIndex", new BasicBSONObject(keysPerIndexKeyValues));
		
		keyValues.put("valid", true);
		keyValues.put("errors", new String[0]);
		if (!full) {
			keyValues.put("warning", "Some checks omitted for speed. use {full:true} option to do more thorough scan.");
		}
		keyValues.put("ok", MongoWP.OK);
		
		BSONDocument reply = new MongoBSONDocument(keyValues);
		messageReplier.replyMessageNoCursor(reply);
	}
	
	@Override
	public void whatsmyuri(String host, int port, MessageReplier messageReplier) {
		Map<String, Object> keyValues = new HashMap<String, Object>();
		
		keyValues.put("you", host + ":" + port);
		keyValues.put("ok", MongoWP.OK);
		
		BSONDocument document = new MongoBSONDocument(keyValues);
		messageReplier.replyMessageNoCursor(document);
	}

	@Override
	public void replSetGetStatus(MessageReplier messageReplier) {
		Map<String, Object> keyValues = new HashMap<String, Object>();
		
		keyValues.put("errmsg", "not running with --replSet");
		keyValues.put("ok", MongoWP.KO);
		
		BSONDocument document = new MongoBSONDocument(keyValues);
		messageReplier.replyMessageNoCursor(document);
	}

	@Override
	public void getLog(GetLogType log, MessageReplier messageReplier) {
		Map<String, Object> keyValues = new HashMap<String, Object>();
		
		if (log == GetLogType.startupWarnings) {
			keyValues.put("totalLinesWritten", 0);
			keyValues.put("log", new String[0]);
			keyValues.put("ok", MongoWP.OK);
		} else {
			keyValues.put("ok", MongoWP.KO);
		}
		
		BSONDocument document = new MongoBSONDocument(keyValues);
		messageReplier.replyMessageNoCursor(document);
	}
	
	@Override
	public void isMaster(MessageReplier messageReplier) {
		Map<String, Object> keyValues = new HashMap<String, Object>();
		
		keyValues.put("ismaster", Boolean.TRUE);
		keyValues.put("maxBsonObjectSize", MongoWP.MAX_BSON_DOCUMENT_SIZE);
		keyValues.put("maxMessageSizeBytes", MongoWP.MAX_MESSAGE_SIZE_BYTES);
		keyValues.put("maxWriteBatchSize", MongoWP.MAX_WRITE_BATCH_SIZE);
		keyValues.put("localTime", System.currentTimeMillis());
		keyValues.put("maxWireVersion", MongoWP.MAX_WIRE_VERSION);
		keyValues.put("minWireVersion", MongoWP.MIN_WIRE_VERSION);
		keyValues.put("ok", MongoWP.OK);
		
		BSONDocument document = new MongoBSONDocument(keyValues);
		messageReplier.replyMessageNoCursor(document);
	}

	@Override
	public void buildInfo(MessageReplier messageReplier) {
		Map<String, Object> keyValues = new HashMap<String, Object>();
		
		keyValues.put(
				"version", MongoWP.VERSION_STRING + " (compatible; ToroDB " + buildProperties.getFullVersion() + ")"
		);
		keyValues.put("gitVersion", buildProperties.getGitCommitId());
		keyValues.put(
				"sysInfo",
				buildProperties.getOsName() + " " + buildProperties.getOsVersion() + " " + buildProperties.getOsArch()
		);
		keyValues.put(
				"versionArray",	Lists.newArrayList(
						buildProperties.getMajorVersion(), buildProperties.getMinorVersion(),
						buildProperties.getSubVersion(), 0
				)
		);
		keyValues.put("bits", "amd64".equals(buildProperties.getOsArch()) ? 64 : 32);
		keyValues.put("debug", false);
		keyValues.put("maxBsonObjectSize", MongoWP.MAX_BSON_DOCUMENT_SIZE);
		keyValues.put("ok", MongoWP.OK);
		
		BSONDocument document = new MongoBSONDocument(keyValues);
		messageReplier.replyMessageNoCursor(document);
	}

    @Override
	public boolean handleError(QueryCommand userCommand, MessageReplier messageReplier, Throwable throwable)
			throws Exception {
		//TODO: Map real mongo error codes		
		ErrorCode errorCode = ErrorCode.INTERNAL_ERROR;
		AttributeMap attributeMap = messageReplier.getAttributeMap();
        LastError lastError = new ToroLastError(
        		RequestOpCode.OP_QUERY, 
        		userCommand, 
        		null, 
        		null, 
        		true, 
        		errorCode);
        attributeMap.attr(ToroRequestProcessor.LAST_ERROR).set(lastError);
        messageReplier.replyQueryCommandFailure(errorCode, throwable.getMessage());
        
		return true;
	}

    @Override
    public void ping(MessageReplier messageReplier) {
        Map<String, Object> keyValues = new HashMap<String, Object>();
        keyValues.put("ok", MongoWP.OK);
        
        BSONDocument document = new MongoBSONDocument(keyValues);
		messageReplier.replyMessageNoCursor(document);
    }

    @Override
    public void listDatabases(MessageReplier messageReplier) throws ExecutionException, InterruptedException, ImplementationDbException{
        AttributeMap attributeMap = messageReplier.getAttributeMap();
        ToroConnection connection = attributeMap.attr(ToroRequestProcessor.CONNECTION).get();
        
        ToroTransaction transaction = connection.createTransaction();
        
        try {
            List<? extends Database> databases
                    = transaction.getDatabases().get();

            double totalSize = 0;
            List<BSONObject> databaseDocs
                    = Lists.newArrayListWithCapacity(databases.size());
            for (Database database : databases) {
                BSONObject databaseDoc = new BasicBSONObject();
                databaseDoc.put("name", database.getName());
                databaseDoc.put("sizeOnDisk", Long.valueOf(database.getSize()).doubleValue());
                //TODO: This is not true, but...
                databaseDoc.put("empty", database.getSize() == 0);

                totalSize += database.getSize();
                databaseDocs.add(databaseDoc);
            }
            BSONObject response = new BasicBSONObject(3);

            response.put("databases", databaseDocs);
            response.put("totalSize", totalSize);
            response.put("ok", MongoWP.OK);

            BSONDocument document = new MongoBSONDocument(response.toMap());
            messageReplier.replyMessageNoCursor(document);
        } finally {
            transaction.close();
        }
    }

    @Override
    public CollStatsReply collStats(CollStatsRequest request) throws Exception {
        CollStatsReply.Builder replyBuilder = new CollStatsReply.Builder(
                request.getDatabase(), 
                request.getCollection());
        replyBuilder
                .setCapped(false)
                .setLastExtentSize(1)
                .setNumExtents(1)
                .setPaddingFactor(0)
                .setScale(request.getScale().intValue())
                .setUsePowerOf2Sizes(false);
                
        AttributeMap attributeMap = request.getAttributes();
        ToroConnection connection = attributeMap.attr(ToroRequestProcessor.CONNECTION).get();
        
        ToroTransaction transaction = connection.createTransaction();
        int scale = replyBuilder.getScale();
        try {
            Collection<? extends NamedToroIndex> indexes
                    = transaction.getIndexes(request.getCollection());
            Map<String, Long> sizeByMap = Maps.newHashMapWithExpectedSize(indexes.size());
            for (NamedToroIndex index : indexes) {
                Long indexSize = transaction.getIndexSize(
                        request.getCollection(), 
                        index.getName()
                ).get();
                sizeByMap.put(index.getName(), indexSize / scale);
            }
            
            replyBuilder.setSizeByIndex(sizeByMap);
            
            replyBuilder.setIdIndexExists(sizeByMap.containsKey("_id"));
            replyBuilder.setCount(
                    transaction.count(
                            request.getCollection(), 
                            TrueQueryCriteria.getInstance()
                    ).get()
            );
            replyBuilder.setSize(
                    transaction.getDocumentsSize(
                            request.getCollection()
                    ).get() / scale
            );
            replyBuilder.setStorageSize(
                    transaction.getCollectionSize(
                            request.getCollection()
                    ).get() / scale
            );
            
            return replyBuilder.build();
        } finally {
            transaction.close();
        }
    }

	@Override
	public void unimplemented(QueryCommand userCommand, MessageReplier messageReplier) throws Exception {
		ErrorCode errorCode = ErrorCode.COMMAND_NOT_SUPPORTED;
		AttributeMap attributeMap = messageReplier.getAttributeMap();
        LastError lastError = new ToroLastError(
        		RequestOpCode.OP_QUERY, 
        		userCommand, 
        		null, 
        		null, 
        		true, 
        		errorCode);
        attributeMap.attr(ToroRequestProcessor.LAST_ERROR).set(lastError);
        messageReplier.replyQueryCommandFailure(errorCode, userCommand.getKey());
	}

    @Override
    public void getnonce(MessageReplier messageReplier) {
        LOGGER.warn("Authentication not supported. Operation 'getnonce' "
                + "called. A fake value is returned");
        BSONObject replyObj = new BasicBSONObject();
        Random r = new Random();
        String nonce = Long.toHexString(r.nextLong());
        replyObj.put("nonce", nonce);
        replyObj.put("ok", MongoWP.OK);
        messageReplier.replyMessageNoCursor(new MongoBSONDocument(replyObj));
    }

    @Override
    public void listCollections(MessageReplier messageReplier, BSONDocument query)
            throws Exception {
        ToroConnection connection = messageReplier.getAttributeMap()
                .attr(ToroRequestProcessor.CONNECTION).get();

        assert query.getValue("listCollections") != null;
        Object filterObject = query.getValue("filter");
        QueryCriteria filter;
        if (filterObject == null) {
            filter = TrueQueryCriteria.getInstance();
        }
        else {
            if (filterObject instanceof BSONObject) {
                filter = queryCriteriaTranslator.translate((BSONObject) filterObject);
            }
            else {
                throw new UserToroException("Filter " + filterObject
                        + " has not been recognized as a valid filter");
            }
        }
        Predicate<DocValue> predicate
                = DocValueQueryCriteriaEvaluator.createPredicate(filter);

        UserCursor<CollectionMetainfo> cursor
                = connection.openCollectionsMetainfoCursor();

        ArrayList<ObjectValue> collectionsInfo = Lists.newArrayList(
                Iterables.filter(
                        Iterables.transform(
                                cursor.readAll(),
                                new CollectionMetainfoToDocValue()
                        ),
                        predicate
                )
        );
        BasicBSONList firstBatch = new BasicBSONList();
        for (ObjectValue colInfo : collectionsInfo) {
            firstBatch.add(MongoValueConverter.translateObject(colInfo));
        }

        BasicBSONObject root = new BasicBSONObject();
        root.append("ok", MongoWP.OK);
        root.append("cursor", new BasicBSONObject()
                .append("id", (long) 0)
                .append("ns", databaseName + ".$cmd.listCollections")
                .append("firstBatch", firstBatch)
        );
        BSONDocument result = new MongoBSONDocument((BSONObject) root);

        messageReplier.replyMessageNoCursor(result);
    }
    
    @Override
    public void listIndexes(MessageReplier messageReplier, String collection)
            throws Exception {
        ToroConnection connection = messageReplier.getAttributeMap()
                .attr(ToroRequestProcessor.CONNECTION).get();
        ToroTransaction transaction = connection.createTransaction();
        
        Collection<? extends NamedToroIndex> indexes = transaction.getIndexes(collection);
        
        BasicBSONList firstBatch = new BasicBSONList();
        
        for (NamedToroIndex index : indexes) {
            String collectionNamespace = databaseName + '.' + collection;
            ObjectValue.Builder objBuider = new ObjectValue.Builder()
                    .putValue("v", 1)
                    .putValue("name", index.getName())
                    .putValue("ns", collectionNamespace)
                    .putValue("key", new ObjectValue.Builder()
                    );
            ObjectValue.Builder keyBuilder = new ObjectValue.Builder();
            for (Map.Entry<AttributeReference, Boolean> entrySet : index.getAttributes().entrySet()) {
                keyBuilder.putValue(
                        entrySet.getKey().toString(),
                        entrySet.getValue() ? 1 : -1
                );
            }
            objBuider.putValue("key", keyBuilder);
            
            firstBatch.add(
                MongoValueConverter.translateObject(objBuider.build())
            );
        }
        
        BasicBSONObject root = new BasicBSONObject();
        root.append("ok", MongoWP.OK);
        root.append("cursor", new BasicBSONObject()
                .append("id", (long) 0)
                .append("ns", databaseName + ".$cmd.listIndexes." + collection)
                .append("firstBatch", firstBatch)
        );
        BSONDocument result = new MongoBSONDocument((BSONObject) root);

        messageReplier.replyMessageNoCursor(result);
    }
    
    private static class CollectionMetainfoToDocValue implements Function<CollectionMetainfo, ObjectValue>{

        @Override
        public ObjectValue apply(CollectionMetainfo input) {
            if (input == null) {
                return null;
            }
            ObjectValue.Builder optionsBuider = new ObjectValue.Builder()
                    .putValue("capped", input.isCapped())
                    .putValue("autoIndexId", false)
                    .putValue("flags", 2)
                    .putValue("storageEngine", input.getStorageEngine());
            if (input.isCapped()) {
                if (input.getMaxSize() > 0) {
                    optionsBuider.putValue("size", input.getMaxSize());
                }
                if (input.getMaxElements() > 0) {
                    optionsBuider.putValue("max", input.getMaxElements());
                }
            }
            
            
            ObjectValue.Builder builder = new ObjectValue.Builder();
            return builder
                    .putValue("name", input.getName())
                    .putValue("options", optionsBuider)
                    .build();
        }
    }
}