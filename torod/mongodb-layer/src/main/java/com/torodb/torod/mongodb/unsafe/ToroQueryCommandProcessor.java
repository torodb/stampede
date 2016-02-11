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
import com.eightkdata.mongowp.bson.BsonDocument;
import com.eightkdata.mongowp.bson.BsonValue;
import com.eightkdata.mongowp.exceptions.CommandNotSupportedException;
import com.eightkdata.mongowp.exceptions.TypesMismatchException;
import com.eightkdata.mongowp.exceptions.UnknownErrorException;
import com.eightkdata.mongowp.server.api.deprecated.QueryCommandProcessor;
import com.eightkdata.mongowp.server.api.deprecated.QueryCommandProcessor.GetLogType;
import com.eightkdata.mongowp.server.api.deprecated.QueryCommandProcessor.QueryCommand;
import com.eightkdata.mongowp.server.callback.MessageReplier;
import com.eightkdata.mongowp.utils.BsonDocumentBuilder;
import com.eightkdata.mongowp.utils.BsonReaderTool;
import com.google.common.collect.Lists;
import com.torodb.torod.core.BuildProperties;
import com.torodb.torod.core.annotations.DatabaseName;
import com.torodb.torod.core.connection.ToroConnection;
import com.torodb.torod.core.connection.ToroTransaction;
import com.torodb.torod.core.connection.TransactionMetainfo;
import com.torodb.torod.core.dbWrapper.exceptions.ImplementationDbException;
import com.torodb.torod.mongodb.MongoLayerConstants;
import com.torodb.torod.mongodb.RequestContext;
import com.torodb.torod.mongodb.translator.QueryCriteriaTranslator;
import io.netty.util.AttributeMap;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import javax.inject.Inject;
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

    @Override
    public void createIndexes(BsonDocument document, MessageReplier messageReplier)
            throws Exception {
        LOGGER.error("The unsafe version of createIndexes command has been called!");
        throw new UnknownErrorException("An unexpected command implementation was called");
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
        LOGGER.error("The unsafe version of create command has been called!");
        throw new UnknownErrorException("An unexpected command implementation was called");
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
		reply.appendUnsafe("firstExtent", newString("2:4b4b000 ns:" + ns)); //TODO(gortiz): Check if correct
		reply.appendUnsafe("lastExtent", newString("2:4b4b000 ns:" + ns)); //TODO(gortiz): Check if correct
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
        LOGGER.error("The unsafe version of listCollections command has been called!");
        throw new UnknownErrorException("An dunexpected command implementation was called");
    }
    
    @Override
    public void listIndexes(MessageReplier messageReplier, String collection)
            throws Exception {
        LOGGER.error("The unsafe version of listIndexescommand has been called!");
        throw new UnknownErrorException("An dunexpected command implementation was called");
    }
}