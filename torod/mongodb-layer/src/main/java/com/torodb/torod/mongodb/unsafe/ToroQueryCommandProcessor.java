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

import com.eightkdata.mongowp.MongoConstants;
import com.eightkdata.mongowp.MongoVersion;
import com.eightkdata.mongowp.bson.BsonArray;
import com.eightkdata.mongowp.bson.BsonDocument;
import com.eightkdata.mongowp.bson.BsonDocument.Entry;
import com.eightkdata.mongowp.bson.BsonValue;
import com.eightkdata.mongowp.exceptions.*;
import com.eightkdata.mongowp.server.api.deprecated.QueryCommandProcessor;
import com.eightkdata.mongowp.server.api.deprecated.QueryCommandProcessor.GetLogType;
import com.eightkdata.mongowp.server.api.deprecated.QueryCommandProcessor.QueryCommand;
import com.eightkdata.mongowp.server.callback.MessageReplier;
import com.eightkdata.mongowp.utils.BsonArrayBuilder;
import com.eightkdata.mongowp.utils.BsonDocumentBuilder;
import com.eightkdata.mongowp.utils.BsonReaderTool;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.torodb.torod.core.BuildProperties;
import com.torodb.torod.core.annotations.DatabaseName;
import com.torodb.torod.core.connection.ToroConnection;
import com.torodb.torod.core.connection.ToroTransaction;
import com.torodb.torod.core.connection.TransactionMetainfo;
import com.torodb.torod.core.dbWrapper.exceptions.ImplementationDbException;
import com.torodb.torod.core.exceptions.ExistentIndexException;
import com.torodb.torod.core.language.AttributeReference;
import com.torodb.torod.core.pojos.IndexedAttributes;
import com.torodb.torod.core.pojos.NamedToroIndex;
import com.torodb.torod.mongodb.MongoLayerConstants;
import com.torodb.torod.mongodb.RequestContext;
import com.torodb.torod.mongodb.translator.QueryCriteriaTranslator;
import io.netty.util.AttributeMap;
import java.util.*;
import java.util.concurrent.ExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.eightkdata.mongowp.bson.utils.DefaultBsonValues.*;

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
        BsonDocumentBuilder reply = new BsonDocumentBuilder();

        BsonValue collectionValue = document.get("createIndexes");
        if (!collectionValue.isString()) {
            reply.appendUnsafe("ok", MongoConstants.BSON_KO);
            reply.appendUnsafe("code", newLong(13111));
            reply.appendUnsafe("errmsg", newString("exception: wrong type for field (createIndexes)"));
            messageReplier.replyMessageNoCursor(reply.build());
            return;
        }
        String collection = collectionValue.asString().getValue();
        BsonValue indexesValue = document.get("indexes");
        if (!indexesValue.isArray()) {
            reply.appendUnsafe("ok", MongoConstants.BSON_KO);
            reply.appendUnsafe("errmsg", newString("indexes has to be an array"));
            messageReplier.replyMessageNoCursor(reply.build());
            return;
        }
        BsonArray uncastedIndexes = indexesValue.asArray();

        ToroConnection connection = getConnection(messageReplier.getAttributeMap());

        int numIndexesBefore;

        try (ToroTransaction transaction
                = connection.createTransaction(TransactionMetainfo.NOT_READ_ONLY)) {

            numIndexesBefore = transaction.getIndexes(collection).size();
            try {
                final Set<String> supportedFields = Sets.newHashSet("name", "key", "unique", "sparse", "ns");
                for (BsonValue<?> uncastedIndexVal : uncastedIndexes) {
                    if (!uncastedIndexVal.isDocument()) {
                        throw new BadValueException("indexes must be an array "
                                + "of documents, but a "
                                + uncastedIndexVal.getType() + " was found");
                    }
                    BsonDocument uncastedIndex = uncastedIndexVal.asDocument();
                    String name = BsonReaderTool.getString(uncastedIndex, "name");
                    BsonDocument key = BsonReaderTool.getDocument(uncastedIndex, "key");
                    boolean unique = BsonReaderTool.getBoolean(uncastedIndex, "unique", false);
                    boolean sparse = BsonReaderTool.getBoolean(uncastedIndex, "sparse", false);
                    String ns = BsonReaderTool.getString(uncastedIndex, "ns", null);

                    if (ns != null) {
                        int firstDot = ns.indexOf('.');
                        if (firstDot < 0 || firstDot == ns.length()) {
                            LOGGER.warn("The index option 'ns' {} does not "
                                    + "conform with the expected '<db>.<col>'. "
                                    + "Ignoring the option", ns);
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

                    Set<String> extraOptions = new HashSet<>();
                    for (Entry<?> entry : uncastedIndex) {
                        String option = entry.getKey();
                        if (!supportedFields.contains(option)) {
                            extraOptions.add(option);
                        }
                    }
                    if (!extraOptions.isEmpty()) {
                        boolean safeExtraOptions = true;
                        for (String extraOption : extraOptions) {
                            if (!extraOption.equals("background") ||
                                    BsonReaderTool.getBoolean(uncastedIndex, "background", false)) {
                                safeExtraOptions = false;
                                break;
                            }
                        }
                        
                        if (!safeExtraOptions) {
                            reply.appendUnsafe("ok", MongoConstants.BSON_KO);
                            String errmsg = "Options "
                                    + extraOptions.toString()
                                    + " are not supported";
                            reply.appendUnsafe("errmsg", newString(errmsg));
                            messageReplier.replyMessageNoCursor(reply.build());
                            return;
                        }
                    }

                    IndexedAttributes.Builder indexedAttsBuilder
                            = new IndexedAttributes.Builder();

                    for (Entry<?> entry : key) {
                        String path = entry.getKey();
                        AttributeReference attRef = parseAttributeReference(path);
                        //TODO: Check that key.get(path) is a number!!
                        boolean ascending = BsonReaderTool.getNumeric(key, path).longValue() > 0;

                        indexedAttsBuilder.addAttribute(attRef, ascending);
                    }

                    transaction.createIndex(collection, name, indexedAttsBuilder.build(), unique, sparse).get();
                }

                int numIndexesAfter = transaction.getIndexes(collection).size();

                transaction.commit().get();

                reply.appendUnsafe("ok", MongoConstants.BSON_OK)
                        .appendUnsafe("createdCollectionAutomatically", FALSE)
                        .appendUnsafe("numIndexesBefore", newInt(numIndexesBefore))
                        .appendUnsafe("numIndexesAfter", newInt(numIndexesAfter));

                messageReplier.replyMessageNoCursor(reply.build());
            }
            catch (ExecutionException ex) {
                if (ex.getCause() instanceof ExistentIndexException) {
                    reply.appendUnsafe("ok", MongoConstants.BSON_OK);
                    reply.appendUnsafe("note", newString(ex.getCause().getMessage()));
                    reply.appendUnsafe("numIndexesBefore", newInt(numIndexesBefore));

                    messageReplier.replyMessageNoCursor(reply.build());
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
        BsonDocumentBuilder reply = new BsonDocumentBuilder();
        
        BsonValue dropIndexesValue = query.get("deleteIndexes");
        BsonValue indexValue = query.get("index");
        
        if (dropIndexesValue == null || !dropIndexesValue.isString()) {
            reply.appendUnsafe("ok", MongoConstants.BSON_KO);
            reply.appendUnsafe("errmsg", newString("The field 'dropIndexes' must be a string"));
            messageReplier.replyMessageNoCursor(reply.build());
            return ;
        }
        if (indexValue == null || !indexValue.isString()) {
            reply.appendUnsafe("ok", MongoConstants.BSON_KO);
            reply.appendUnsafe("errmsg", newString("The field 'index' must be a string"));
            messageReplier.replyMessageNoCursor(reply.build());
            return ;
        }
        
        String collection = dropIndexesValue.asString().getValue();
        String indexName = indexValue.asString().getValue();
        
        ToroConnection connection = getConnection(messageReplier.getAttributeMap());
        
        try (ToroTransaction transaction
                = connection.createTransaction(TransactionMetainfo.NOT_READ_ONLY)) {
            
            if (indexName.equals("*")) { //TODO: Support * in deleteIndexes
                reply.appendUnsafe("ok", MongoConstants.BSON_KO);
                reply.appendUnsafe("errmsg", newString("The wildcard '*' is not supported by ToroDB right now"));
                messageReplier.replyMessageNoCursor(reply.build());
                return ;
            }
        
            Boolean removed = transaction.dropIndex(collection, indexName).get();
            if (!removed) {
                reply.appendUnsafe("ok", MongoConstants.BSON_KO);
                reply.appendUnsafe("errmsg", newString("index not found with name ["+indexName+"]"));
                messageReplier.replyMessageNoCursor(reply.build());
                return ;
            }
            
            transaction.commit();
            
            reply.appendUnsafe("ok", MongoConstants.BSON_OK);
            messageReplier.replyMessageNoCursor(reply.build());
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

		BsonDocumentBuilder reply = new BsonDocumentBuilder();
		if(connection.createCollection(collection, null)) {
			reply.appendUnsafe("ok", MongoConstants.BSON_OK);
		} else {
			reply.appendUnsafe("ok", MongoConstants.BSON_KO);
			reply.appendUnsafe("errmsg", newString("collection already exists"));
		}

		messageReplier.replyMessageNoCursor(reply.build());
	}

	@Override
	public void getLastError(Object w, boolean j, 
			boolean fsync, int wtimeout, MessageReplier messageReplier) throws Exception {
        LOGGER.error("The unsafe version of GetLastError command has been called!");
        throw new UnknownErrorException("An unexpected command implementation was called");
	}

	@Override
	public void validate(String database, BsonDocument document, MessageReplier messageReplier) throws TypesMismatchException {
		BsonDocumentBuilder reply = new BsonDocumentBuilder();
		
		String collection = document.get("validate").asString().getValue();
		boolean full = BsonReaderTool.getBoolean(document, "full", false);
		String ns = database + "." + collection;
		
		reply.appendUnsafe("ns", newString(ns));
		reply.appendUnsafe("firstExtent", newString("2:4b4b000 ns:" + ns)); //TODO(gortiz): Check if that is correct
		reply.appendUnsafe("lastExtent", newString("2:4b4b000 ns:" + ns)); //TODO(gortiz): Check if that is correct
		reply.appendUnsafe("extentCount", newInt(1));
		reply.appendUnsafe("datasize", newInt(0));
		reply.appendUnsafe("nrecords", newInt(0));
		reply.appendUnsafe("lastExtentSize", newInt(8192));
		reply.appendUnsafe("padding", newInt(0));
		
		BsonDocumentBuilder firstExtentDetailsKeyValues = new BsonDocumentBuilder();
		firstExtentDetailsKeyValues.appendUnsafe("loc", newString("2:4b4b000"));
		firstExtentDetailsKeyValues.appendUnsafe("xnext", NULL);
		firstExtentDetailsKeyValues.appendUnsafe("xprev", NULL);
		firstExtentDetailsKeyValues.appendUnsafe("nsdiag", newString(ns));
		firstExtentDetailsKeyValues.appendUnsafe("size", newInt(8192));
		firstExtentDetailsKeyValues.appendUnsafe("firstRecord", NULL);
		firstExtentDetailsKeyValues.appendUnsafe("firstRecord", NULL);
		reply.appendUnsafe("firstExtentDetails", firstExtentDetailsKeyValues.build());
		
		reply.appendUnsafe("deletedCount", newInt(0));
		reply.appendUnsafe("deletedSize", newInt(0));
		reply.appendUnsafe("nIndexes", newInt(1));

		BsonDocumentBuilder keysPerIndexKeyValues = new BsonDocumentBuilder();
		keysPerIndexKeyValues.appendUnsafe(ns + ".$_id_", newInt(0));
		reply.appendUnsafe("keysPerIndex", keysPerIndexKeyValues.build());
		
		reply.appendUnsafe("valid", TRUE);
		reply.appendUnsafe("errors", EMPTY_ARRAY);
		if (!full) {
			reply.appendUnsafe("warning",
                    newString(
                        "Some checks omitted for speed. use {full:true} option to "
                                + "do more thorough scan."
                    )
            );
		}
		reply.appendUnsafe("ok", MongoConstants.BSON_OK);
		
		messageReplier.replyMessageNoCursor(reply.build());
	}
	
	@Override
	public void whatsmyuri(String host, int port, MessageReplier messageReplier) {
		BsonDocumentBuilder reply = new BsonDocumentBuilder();
		
		reply.appendUnsafe("you", newString(host + ":" + port));
		reply.appendUnsafe("ok", MongoConstants.BSON_OK);
		
		messageReplier.replyMessageNoCursor(reply.build());
	}

	@Override
	public void replSetGetStatus(MessageReplier messageReplier) {
		BsonDocumentBuilder reply = new BsonDocumentBuilder();
		
		reply.appendUnsafe("errmsg", newString("not running with --replSet"));
		reply.appendUnsafe("ok", MongoConstants.BSON_KO);
		
		messageReplier.replyMessageNoCursor(reply.build());
	}

	@Override
	public void getLog(GetLogType log, MessageReplier messageReplier) {
		BsonDocumentBuilder reply = new BsonDocumentBuilder();
		
		if (log == GetLogType.startupWarnings) {
			reply.appendUnsafe("totalLinesWritten", newInt(0));
			reply.appendUnsafe("log", EMPTY_ARRAY);
			reply.appendUnsafe("ok", MongoConstants.BSON_OK);
		} else {
			reply.appendUnsafe("ok", MongoConstants.BSON_KO);
		}
		
		messageReplier.replyMessageNoCursor(reply.build());
	}
	
	@Override
	public void isMaster(MessageReplier messageReplier) {
		BsonDocumentBuilder reply = new BsonDocumentBuilder();
		
		reply.appendUnsafe("ismaster", TRUE);
		reply.appendUnsafe("maxBsonObjectSize", newInt(MongoLayerConstants.MAX_BSON_DOCUMENT_SIZE));
		reply.appendUnsafe("maxMessageSizeBytes", newInt(MongoLayerConstants.MAX_MESSAGE_SIZE_BYTES));
		reply.appendUnsafe("maxWriteBatchSize", newInt(MongoLayerConstants.MAX_WRITE_BATCH_SIZE));
		reply.appendUnsafe("localTime", newDateTime(System.currentTimeMillis()));
		reply.appendUnsafe("maxWireVersion", newInt(MongoLayerConstants.MAX_WIRE_VERSION));
		reply.appendUnsafe("minWireVersion", newInt(MongoLayerConstants.MIN_WIRE_VERSION));
		reply.appendUnsafe("ok", MongoConstants.BSON_OK);
		
		messageReplier.replyMessageNoCursor(reply.build());
	}

	@Override
	public void buildInfo(MessageReplier messageReplier) {
		BsonDocumentBuilder reply = new BsonDocumentBuilder();
		
		reply.appendUnsafe(
				"version", 
                newString(MongoLayerConstants.VERSION_STRING + " (compatible; ToroDB " + buildProperties.getFullVersion() + ")")
		);
		reply.appendUnsafe("gitVersion", newString(buildProperties.getGitCommitId()));
		reply.appendUnsafe(
				"sysInfo",
				newString(
                        buildProperties.getOsName() + " " + buildProperties.getOsVersion() + " " + buildProperties.getOsArch()
                )
		);
		reply.appendUnsafe(
				"versionArray",
                newArray(
                        Lists.<BsonValue<?>>newArrayList(
                                newInt(MongoVersion.V3_0.getMajor()),
                                newInt(MongoVersion.V3_0.getMinor()),
                                newInt(0),
                                newInt(0)
                        )
                )
		);
		reply.appendUnsafe("bits", newInt("amd64".equals(buildProperties.getOsArch()) ? 64 : 32));
		reply.appendUnsafe("debug", FALSE);
		reply.appendUnsafe("maxBsonObjectSize", newInt(MongoLayerConstants.MAX_BSON_DOCUMENT_SIZE));
		reply.appendUnsafe("ok", MongoConstants.BSON_OK);
		
		messageReplier.replyMessageNoCursor(reply.build());
	}

    @Override
    public void ping(MessageReplier messageReplier) {
        BsonDocumentBuilder reply = new BsonDocumentBuilder();
        reply.appendUnsafe("ok", MongoConstants.BSON_OK);
        
		messageReplier.replyMessageNoCursor(reply.build());
    }

    @Override
    public void listDatabases(MessageReplier messageReplier) throws ExecutionException, InterruptedException, ImplementationDbException, UnknownErrorException{
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
        BsonDocumentBuilder replyObj = new BsonDocumentBuilder();
        Random r = new Random();
        String nonce = Long.toHexString(r.nextLong());
        replyObj.appendUnsafe("nonce", newString(nonce));
        replyObj.appendUnsafe("ok", MongoConstants.BSON_OK);
        messageReplier.replyMessageNoCursor(replyObj.build());
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

        BsonArrayBuilder firstBatch = new BsonArrayBuilder();

        for (NamedToroIndex index : indexes) {
            String collectionNamespace = databaseName + '.' + collection;

            BsonDocumentBuilder objBuilder = new BsonDocumentBuilder()
                    .appendUnsafe("v", newInt(1))
                    .appendUnsafe("name", newString(index.getName()))
                    .appendUnsafe("ns", newString(collectionNamespace));
            BsonDocumentBuilder keyBuilder = new BsonDocumentBuilder();
            for (Map.Entry<AttributeReference, Boolean> entrySet : index.getAttributes().entrySet()) {
                keyBuilder.appendUnsafe(
                        entrySet.getKey().toString(),
                        newInt(entrySet.getValue() ? 1 : -1)
                );
            }
            objBuilder.appendUnsafe("key", keyBuilder.build());

            firstBatch.add(objBuilder.build());
        }

        BsonDocumentBuilder root = new BsonDocumentBuilder();
        root.appendUnsafe("ok", MongoConstants.BSON_OK);
        root.appendUnsafe("cursor", new BsonDocumentBuilder()
                .appendUnsafe("id", newLong(0))
                .appendUnsafe("ns", newString(databaseName + ".$cmd.listIndexes." + collection))
                .appendUnsafe("firstBatch", firstBatch.build())
                .build()
        );
        messageReplier.replyMessageNoCursor(root.build());
    }
}