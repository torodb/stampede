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

package com.torodb.torod.mongodb.unsafe;

import com.eightkdata.mongowp.mongoserver.MongoVersion;
import com.eightkdata.mongowp.mongoserver.api.QueryCommandProcessor;
import com.eightkdata.mongowp.mongoserver.api.commands.CollStatsReply;
import com.eightkdata.mongowp.mongoserver.api.commands.CollStatsRequest;
import com.eightkdata.mongowp.mongoserver.api.commands.CountReply;
import com.eightkdata.mongowp.mongoserver.api.commands.CountRequest;
import com.eightkdata.mongowp.mongoserver.api.safe.tools.bson.BsonReaderTool;
import com.eightkdata.mongowp.mongoserver.callback.MessageReplier;
import com.eightkdata.mongowp.mongoserver.protocol.MongoWP;
import com.eightkdata.mongowp.mongoserver.protocol.exceptions.*;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;
import com.google.inject.Inject;
import com.torodb.kvdocument.conversion.mongo.MongoValueConverter;
import com.torodb.kvdocument.values.ObjectValue;
import com.torodb.torod.core.BuildProperties;
import com.torodb.torod.core.annotations.DatabaseName;
import com.torodb.torod.core.connection.ToroConnection;
import com.torodb.torod.core.connection.ToroTransaction;
import com.torodb.torod.core.connection.TransactionMetainfo;
import com.torodb.torod.core.dbWrapper.exceptions.ImplementationDbException;
import com.torodb.torod.core.exceptions.ExistentIndexException;
import com.torodb.torod.core.language.AttributeReference;
import com.torodb.torod.core.language.querycriteria.QueryCriteria;
import com.torodb.torod.core.language.querycriteria.TrueQueryCriteria;
import com.torodb.torod.core.pojos.CollectionMetainfo;
import com.torodb.torod.core.pojos.IndexedAttributes;
import com.torodb.torod.core.pojos.NamedToroIndex;
import com.torodb.torod.mongodb.MongoLayerConstants;
import com.torodb.torod.mongodb.RequestContext;
import com.torodb.torod.mongodb.translator.QueryCriteriaTranslator;
import io.netty.util.AttributeMap;
import java.util.*;
import java.util.concurrent.ExecutionException;
import org.bson.*;
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

    private ToroConnection getConnection(AttributeMap attMap) {
        return RequestContext.getFrom(attMap).getToroConnection();
    }

    @Override
    public CountReply count(CountRequest request) throws Exception {
        ToroConnection connection = getConnection(request.getAttributes());

    	QueryCriteria queryCriteria;
        if (request.getQuery() == null) {
            queryCriteria = TrueQueryCriteria.getInstance();
        } else {
            queryCriteria = queryCriteriaTranslator.translate(
                request.getQuery()
            );
        }
        
        try (ToroTransaction transaction
                = connection.createTransaction(TransactionMetainfo.READ_ONLY)) {
            Integer count = transaction.count(
                    request.getCollection(), 
                    queryCriteria
            ).get();
            
            return new CountReply(count);
        }
	}

    @Override
    public com.eightkdata.mongowp.mongoserver.api.pojos.InsertResponse insert(BsonDocument document, AttributeMap attributeMap)
            throws Exception {
        LOGGER.error("The unsafe version of insert command has been called!");
        throw new UnknownErrorException("An unexpected command implementation was called");
	}

    @Override
    public void update(BsonDocument document, MessageReplier messageReplier)
            throws Exception {
        LOGGER.error("The unsafe version of insert command has been called!");
        throw new UnknownErrorException("An unexpected command implementation was called");
	}

	@Override
    public void delete(BsonDocument document, MessageReplier messageReplier)
            throws Exception {
        LOGGER.error("The unsafe version of delete command has been called!");
        throw new UnknownErrorException("An unexpected command implementation was called");
	}

    @Override
    public void drop(BsonDocument document, MessageReplier messageReplier)
            throws Exception {
        LOGGER.error("The unsafe version of insert command has been called!");
        throw new UnknownErrorException("An unexpected command implementation was called");
	}
    
    private AttributeReference parseAttributeReference(String path) {
        AttributeReference.Builder builder = new AttributeReference.Builder();
        
        StringTokenizer st = new StringTokenizer(path, ".");
        while (st.hasMoreTokens()) {
            builder.addObjectKey(st.nextToken());
        }
        return builder.build();
    }

    @Override
    public void createIndexes(BsonDocument document, MessageReplier messageReplier)
            throws Exception {
        BsonDocument reply = new BsonDocument();

        BsonValue collectionValue = document.get("createIndexes");
        if (!collectionValue.isString()) {
            reply.put("ok", MongoWP.BSON_KO);
            reply.put("code", new BsonInt64(13111));
            reply.put("errmsg", new BsonString("exception: wrong type for field (createIndexes)"));
            messageReplier.replyMessageNoCursor(reply);
            return;
        }
        String collection = collectionValue.asString().getValue();
        BsonValue indexesValue = document.get("indexes");
        if (!indexesValue.isArray()) {
            reply.put("ok", MongoWP.BSON_KO);
            reply.put("errmsg", new BsonString("indexes has to be an array"));
            messageReplier.replyMessageNoCursor(reply);
            return;
        }
        //TODO: Unsafe cast
        List<BsonDocument> uncastedIndexes = (List<BsonDocument>) indexesValue;

        ToroConnection connection = getConnection(messageReplier.getAttributeMap());

        int numIndexesBefore;

        try (ToroTransaction transaction
                = connection.createTransaction(TransactionMetainfo.NOT_READ_ONLY)) {

            numIndexesBefore = transaction.getIndexes(collection).size();
            try {
                final Set<String> supportedFields = Sets.newHashSet("name", "key", "unique", "sparse", "ns");
                for (BsonDocument uncastedIndex : uncastedIndexes) {
                    String name = BsonReaderTool.getString(uncastedIndex, "name");
                    BsonDocument key = BsonReaderTool.getDocument(uncastedIndex, "key");
                    boolean unique = BsonReaderTool.getBoolean(uncastedIndex, "unique", false);
                    boolean sparse = BsonReaderTool.getBoolean(uncastedIndex, "sparse", false);
                    String ns = BsonReaderTool.getString(uncastedIndex, "ns", null);

                    if (ns != null) {
                        int firstDot = ns.indexOf('.');
                        if (firstDot < 0 || firstDot == ns.length()) {
                            LOGGER.warn("The index option 'ns' {} does not conform with the expected '<db>.<col>'. Ignoring the option", ns);
                        }
                        else {
                            String nsDatabase = ns.substring(0, firstDot);
                            String nsCollection = ns.substring(firstDot + 1);
                            if (!nsDatabase.equals(databaseName)) {
                                throw new CommandFailed("createIndex",
                                        "Trying to create an index whose "
                                        + "namespace is on the unsupported "
                                        + "database " + nsDatabase);
                            }
                            if (!nsCollection.equals(collection)) {
                                throw new CommandFailed("createIndex",
                                        "Trying to create an index whose "
                                        + "namespace (" + nsCollection
                                        + ")is on "
                                        + "different collection than one on "
                                        + "which the command has been called ("
                                        + collection + ")");
                            }
                        }
                    }

                    SetView<String> extraOptions = Sets.difference(uncastedIndex.keySet(), supportedFields);
                    if (!extraOptions.isEmpty()) {
                        reply.put("ok", MongoWP.BSON_KO);
                        String errmsg = "Options "
                                + extraOptions.toString()
                                + " are not supported";
                        reply.put("errmsg", new BsonString(errmsg));
                        messageReplier.replyMessageNoCursor(reply);
                        return;
                    }

                    IndexedAttributes.Builder indexedAttsBuilder
                            = new IndexedAttributes.Builder();

                    for (String path : key.keySet()) {
                        AttributeReference attRef = parseAttributeReference(path);
                        //TODO: Check that key.get(path) is a number!!
                        boolean ascending = BsonReaderTool.getNumeric(key, path).longValue() > 0;

                        indexedAttsBuilder.addAttribute(attRef, ascending);
                    }

                    transaction.createIndex(collection, name, indexedAttsBuilder.build(), unique, sparse).get();
                }

                int numIndexesAfter = transaction.getIndexes(collection).size();

                transaction.commit().get();

                reply.put("ok", MongoWP.BSON_OK);
                reply.put("createdCollectionAutomatically", BsonBoolean.FALSE);
                reply.put("numIndexesBefore", new BsonInt32(numIndexesBefore));
                reply.put("numIndexesAfter", new BsonInt32(numIndexesAfter));

                messageReplier.replyMessageNoCursor(reply);
            }
            catch (ExecutionException ex) {
                if (ex.getCause() instanceof ExistentIndexException) {
                    reply.put("ok", MongoWP.BSON_OK);
                    reply.put("note", new BsonString(ex.getCause().getMessage()));
                    reply.put("numIndexesBefore", new BsonInt32(numIndexesBefore));

                    messageReplier.replyMessageNoCursor(reply);
                }
                else {
                    throw ex;
                }
            }
        }

    }

    @Override
    public void deleteIndexes(BsonDocument query, MessageReplier messageReplier)
            throws Exception {
        BsonDocument reply = new BsonDocument();
        
        BsonValue dropIndexesValue = query.get("deleteIndexes");
        BsonValue indexValue = query.get("index");
        
        if (!dropIndexesValue.isString()) {
            reply.put("ok", MongoWP.BSON_KO);
            reply.put("errmsg", new BsonString("The field 'dropIndexes' must be a string"));
            messageReplier.replyMessageNoCursor(reply);
            return ;
        }
        if (!indexValue.isString()) {
            reply.put("ok", MongoWP.BSON_KO);
            reply.put("errmsg", new BsonString("The field 'index' must be a string"));
            messageReplier.replyMessageNoCursor(reply);
            return ;
        }
        
        String collection = dropIndexesValue.asString().getValue();
        String indexName = indexValue.asString().getValue();
        
        ToroConnection connection = getConnection(messageReplier.getAttributeMap());
        
        try (ToroTransaction transaction
                = connection.createTransaction(TransactionMetainfo.NOT_READ_ONLY)) {
            
            if (indexName.equals("*")) { //TODO: Support * in deleteIndexes
                reply.put("ok", MongoWP.BSON_KO);
                reply.put("errmsg", new BsonString("The wildcard '*' is not supported by ToroDB right now"));
                messageReplier.replyMessageNoCursor(reply);
                return ;
            }
        
            Boolean removed = transaction.dropIndex(collection, indexName).get();
            if (!removed) {
                reply.put("ok", MongoWP.BSON_KO);
                reply.put("errmsg", new BsonString("index not found with name ["+indexName+"]"));
                messageReplier.replyMessageNoCursor(reply);
                return ;
            }
            
            transaction.commit();
            
            reply.put("ok", MongoWP.BSON_OK);
            messageReplier.replyMessageNoCursor(reply);
        }
    }
	
	@Override
	public void create(BsonDocument document, MessageReplier messageReplier)
			throws Exception {
		String collection = document.get("create").asString().getValue();
		boolean capped = BsonReaderTool.getBoolean(document, "capped", true);

		if (capped) { // Other flags silently ignored
            throw new OperationFailedException("Capped collections are not supported yet");
		}

		ToroConnection connection = getConnection(messageReplier.getAttributeMap());

		BsonDocument reply = new BsonDocument();
		if(connection.createCollection(collection, null)) {
			reply.put("ok", MongoWP.BSON_OK);
		} else {
			reply.put("ok", MongoWP.BSON_KO);
			reply.put("errmsg", new BsonString("collection already exists"));
		}

		messageReplier.replyMessageNoCursor(reply);
	}

	@Override
	public void getLastError(Object w, boolean j, 
			boolean fsync, int wtimeout, MessageReplier messageReplier) throws Exception {
        LOGGER.error("The unsafe version of GetLastError command has been called!");
        throw new UnknownErrorException("An unexpected command implementation was called");
	}

	@Override
	public void validate(String database, BsonDocument document, MessageReplier messageReplier) throws TypesMismatchException {
		BsonDocument reply = new BsonDocument();
		
		String collection = document.get("validate").asString().getValue();
		boolean full = BsonReaderTool.getBoolean(document, "full", false);
		String ns = database + "." + collection;
		
		reply.put("ns", new BsonString(ns));
		reply.put("firstExtent", new BsonString("2:4b4b000 ns:" + ns)); //TODO(gortiz): Check if that is correct
		reply.put("lastExtent", new BsonString("2:4b4b000 ns:" + ns)); //TODO(gortiz): Check if that is correct
		reply.put("extentCount", new BsonInt32(1));
		reply.put("datasize", new BsonInt32(0));
		reply.put("nrecords", new BsonInt32(0));
		reply.put("lastExtentSize", new BsonInt32(8192));
		reply.put("padding", new BsonInt32(0));
		
		BsonDocument firstExtentDetailsKeyValues = new BsonDocument();
		firstExtentDetailsKeyValues.put("loc", new BsonString("2:4b4b000"));
		firstExtentDetailsKeyValues.put("xnext", BsonNull.VALUE);
		firstExtentDetailsKeyValues.put("xprev", BsonNull.VALUE);
		firstExtentDetailsKeyValues.put("nsdiag", new BsonString(ns));
		firstExtentDetailsKeyValues.put("size", new BsonInt32(8192));
		firstExtentDetailsKeyValues.put("firstRecord", BsonNull.VALUE);
		firstExtentDetailsKeyValues.put("firstRecord", BsonNull.VALUE);
		reply.put("firstExtentDetails", firstExtentDetailsKeyValues);
		
		reply.put("deletedCount", new BsonInt32(0));
		reply.put("deletedSize", new BsonInt32(0));
		reply.put("nIndexes", new BsonInt32(1));

		BsonDocument keysPerIndexKeyValues = new BsonDocument();
		keysPerIndexKeyValues.put(ns + ".$_id_", new BsonInt32(0));
		reply.put("keysPerIndex", keysPerIndexKeyValues);
		
		reply.put("valid", BsonBoolean.TRUE);
		reply.put("errors", new BsonArray());
		if (!full) {
			reply.put("warning",
                    new BsonString(
                        "Some checks omitted for speed. use {full:true} option to "
                                + "do more thorough scan."
                    )
            );
		}
		reply.put("ok", MongoWP.BSON_OK);
		
		messageReplier.replyMessageNoCursor(reply);
	}
	
	@Override
	public void whatsmyuri(String host, int port, MessageReplier messageReplier) {
		BsonDocument reply = new BsonDocument();
		
		reply.put("you", new BsonString(host + ":" + port));
		reply.put("ok", MongoWP.BSON_OK);
		
		messageReplier.replyMessageNoCursor(reply);
	}

	@Override
	public void replSetGetStatus(MessageReplier messageReplier) {
		BsonDocument reply = new BsonDocument();
		
		reply.put("errmsg", new BsonString("not running with --replSet"));
		reply.put("ok", MongoWP.BSON_KO);
		
		messageReplier.replyMessageNoCursor(reply);
	}

	@Override
	public void getLog(GetLogType log, MessageReplier messageReplier) {
		BsonDocument reply = new BsonDocument();
		
		if (log == GetLogType.startupWarnings) {
			reply.put("totalLinesWritten", new BsonInt32(0));
			reply.put("log", new BsonArray());
			reply.put("ok", MongoWP.BSON_OK);
		} else {
			reply.put("ok", MongoWP.BSON_KO);
		}
		
		messageReplier.replyMessageNoCursor(reply);
	}
	
	@Override
	public void isMaster(MessageReplier messageReplier) {
		BsonDocument reply = new BsonDocument();
		
		reply.put("ismaster", BsonBoolean.TRUE);
		reply.put("maxBsonObjectSize", new BsonInt32(MongoLayerConstants.MAX_BSON_DOCUMENT_SIZE));
		reply.put("maxMessageSizeBytes", new BsonInt32(MongoLayerConstants.MAX_MESSAGE_SIZE_BYTES));
		reply.put("maxWriteBatchSize", new BsonInt32(MongoLayerConstants.MAX_WRITE_BATCH_SIZE));
		reply.put("localTime", new BsonDateTime(System.currentTimeMillis()));
		reply.put("maxWireVersion", new BsonInt32(MongoLayerConstants.MAX_WIRE_VERSION));
		reply.put("minWireVersion", new BsonInt32(MongoLayerConstants.MIN_WIRE_VERSION));
		reply.put("ok", MongoWP.BSON_OK);
		
		messageReplier.replyMessageNoCursor(reply);
	}

	@Override
	public void buildInfo(MessageReplier messageReplier) {
		BsonDocument reply = new BsonDocument();
		
		reply.put(
				"version", 
                new BsonString(MongoLayerConstants.VERSION_STRING + " (compatible; ToroDB " + buildProperties.getFullVersion() + ")")
		);
		reply.put("gitVersion", new BsonString(buildProperties.getGitCommitId()));
		reply.put(
				"sysInfo",
				new BsonString(
                        buildProperties.getOsName() + " " + buildProperties.getOsVersion() + " " + buildProperties.getOsArch()
                )
		);
		reply.put(
				"versionArray",
                new BsonArray(
                        Lists.newArrayList(
                                new BsonInt32(MongoVersion.V3_0.getMajor()),
                                new BsonInt32(MongoVersion.V3_0.getMinor()),
                                new BsonInt32(0),
                                new BsonInt32(0)
                        )
                )
		);
		reply.put("bits", new BsonInt32("amd64".equals(buildProperties.getOsArch()) ? 64 : 32));
		reply.put("debug", BsonBoolean.FALSE);
		reply.put("maxBsonObjectSize", new BsonInt32(MongoLayerConstants.MAX_BSON_DOCUMENT_SIZE));
		reply.put("ok", MongoWP.BSON_OK);
		
		messageReplier.replyMessageNoCursor(reply);
	}

    @Override
	public boolean handleError(QueryCommand userCommand, MessageReplier messageReplier, Throwable throwable)
			throws Exception {
//		ErrorCode errorCode = ErrorCode.INTERNAL_ERROR;
//		AttributeMap attributeMap = messageReplier.getAttributeMap();
//        WriteOpResult lastError = new ToroLastError(
//        		RequestOpCode.OP_QUERY,
//        		userCommand,
//        		null,
//        		null,
//        		true,
//        		errorCode);
//        attributeMap.attr(ToroRequestProcessor.LAST_ERROR).set(lastError);
//        messageReplier.replyQueryCommandFailure(errorCode, throwable.getMessage());
//
//		return true;

        throw new RuntimeException("This version of HandleError should not be called", throwable);
	}

    @Override
    public void ping(MessageReplier messageReplier) {
        BsonDocument reply = new BsonDocument();
        reply.put("ok", MongoWP.BSON_OK);
        
		messageReplier.replyMessageNoCursor(reply);
    }

    @Override
    public void listDatabases(MessageReplier messageReplier) throws ExecutionException, InterruptedException, ImplementationDbException, UnknownErrorException{
        LOGGER.error("The unsafe version of insert command has been called!");
        throw new UnknownErrorException("An unexpected command implementation was called");
    }

    @Override
    public CollStatsReply collStats(CollStatsRequest request) throws Exception {
        LOGGER.error("The unsafe version of insert command has been called!");
        throw new UnknownErrorException("An unexpected command implementation was called");
    }

	@Override
	public void unimplemented(QueryCommand userCommand, MessageReplier messageReplier) throws Exception {
        throw new CommandNotSupportedException(userCommand.getKey());
	}

    @Override
    public void getnonce(MessageReplier messageReplier) {
        LOGGER.warn("Authentication not supported. Operation 'getnonce' "
                + "called. A fake value is returned");
        BsonDocument replyObj = new BsonDocument();
        Random r = new Random();
        String nonce = Long.toHexString(r.nextLong());
        replyObj.put("nonce", new BsonString(nonce));
        replyObj.put("ok", MongoWP.BSON_OK);
        messageReplier.replyMessageNoCursor(replyObj);
    }

    @Override
    public void listCollections(MessageReplier messageReplier, BsonDocument query)
            throws Exception {
        LOGGER.error("The unsafe version of insert command has been called!");
        throw new UnknownErrorException("An dunexpected command implementation was called");
    }
    
    @Override
    public void listIndexes(MessageReplier messageReplier, String collection)
            throws Exception {
        ToroConnection connection = getConnection(messageReplier.getAttributeMap());
        Collection<? extends NamedToroIndex> indexes;
        try (ToroTransaction transaction
                = connection.createTransaction(TransactionMetainfo.READ_ONLY)) {

            indexes = transaction.getIndexes(collection);
        }

        BsonArray firstBatch = new BsonArray();

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

        BsonDocument root = new BsonDocument();
        root.append("ok", MongoWP.BSON_OK);
        root.append("cursor", new BsonDocument()
                .append("id", new BsonInt64(0))
                .append("ns", new BsonString(databaseName + ".$cmd.listIndexes." + collection))
                .append("firstBatch", firstBatch)
        );
        messageReplier.replyMessageNoCursor(root);
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