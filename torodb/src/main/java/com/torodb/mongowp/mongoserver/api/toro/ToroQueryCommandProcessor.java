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

import io.netty.util.AttributeMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import javax.annotation.Nonnull;

import org.bson.BSONObject;
import org.bson.BasicBSONObject;

import com.mongodb.WriteConcern;
import com.torodb.mongowp.mongoserver.api.toro.util.BSONDocuments;
import com.torodb.mongowp.mongoserver.api.toro.util.BSONToroDocument;
import com.torodb.torod.core.Session;
import com.torodb.torod.core.WriteFailMode;
import com.torodb.torod.core.connection.DeleteResponse;
import com.torodb.torod.core.connection.InsertResponse;
import com.torodb.torod.core.connection.ToroConnection;
import com.torodb.torod.core.connection.ToroTransaction;
import com.torodb.torod.core.connection.UpdateResponse;
import com.torodb.torod.core.cursors.CursorId;
import com.torodb.torod.core.dbMetaInf.DbMetaInformationCache;
import com.torodb.torod.core.executor.ExecutorFactory;
import com.torodb.torod.core.executor.SessionExecutor;
import com.torodb.torod.core.language.operations.DeleteOperation;
import com.torodb.torod.core.language.operations.UpdateOperation;
import com.torodb.torod.core.language.projection.Projection;
import com.torodb.torod.core.language.querycriteria.QueryCriteria;
import com.torodb.torod.core.language.querycriteria.TrueQueryCriteria;
import com.torodb.torod.core.language.update.UpdateAction;
import com.torodb.torod.core.subdocument.ToroDocument;
import com.torodb.torod.d2r.DefaultD2RTranslator;
import com.torodb.translator.QueryCriteriaTranslator;
import com.torodb.translator.QueryEncapsulation;
import com.torodb.translator.QueryModifier;
import com.torodb.translator.QuerySortOrder;
import com.torodb.translator.UpdateActionTranslator;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import com.eightkdata.mongowp.messages.request.RequestOpCode;
import com.eightkdata.mongowp.mongoserver.api.QueryCommandProcessor;
import com.eightkdata.mongowp.mongoserver.api.callback.LastError;
import com.eightkdata.mongowp.mongoserver.api.callback.MessageReplier;
import com.eightkdata.mongowp.mongoserver.api.commands.AdministrationQueryCommand;
import com.eightkdata.mongowp.mongoserver.api.commands.QueryAndWriteOperationsQueryCommand;
import com.eightkdata.mongowp.mongoserver.protocol.MongoWP;
import com.eightkdata.mongowp.mongoserver.protocol.MongoWP.ErrorCode;
import com.eightkdata.nettybson.api.BSONDocument;
import com.eightkdata.nettybson.mongodriver.MongoBSONDocument;

/**
 *
 */
public class ToroQueryCommandProcessor implements QueryCommandProcessor {

	// TODO: implement with toro natives
	@Override
	@SuppressFBWarnings("NP_LOAD_OF_KNOWN_NULL_VALUE")
	public void count(BSONDocument document, MessageReplier messageReplier)
			throws Exception {
		AttributeMap attributeMap = messageReplier.getAttributeMap();
		ToroConnection connection = attributeMap.attr(
				ToroRequestProcessor.CONNECTION).get();

		Map<String, Object> keyValues = new HashMap<String, Object>();

		String collection = ToroCollectionTranslator
				.translate((String) document.getValue("count"));
		QueryCriteriaTranslator queryCriteriaTranslator = new QueryCriteriaTranslator();
		BSONObject query = (BSONObject) document.getValue("query");
		for (String key : query.keySet()) {
			if (QueryModifier.getByKey(key) != null
					|| QuerySortOrder.getByKey(key) != null) {
				throw new Exception("Modifier " + key + " not supported");
			}
		}
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
		int numberToSkip = document.hasKey("skip") ? ((Number) document
				.getValue("skip")).intValue() : 0;
		int limit = document.hasKey("limit") ? ((Number) document
				.getValue("limit")).intValue() : 0;
		boolean autoclose = true;
		boolean hasTimeout = false;

		CursorId cursorId = null;
		BSONDocuments results = null;

		limit = 0;

		ToroTransaction transaction = connection.createTransaction();

		try {
			if (limit == 0) {
				cursorId = transaction.query(collection, queryCriteria,
						projection, numberToSkip, autoclose, hasTimeout);
			} else {
				cursorId = transaction.query(collection, queryCriteria,
						projection, numberToSkip, limit, autoclose, hasTimeout);
			}

			try {
				int count = 0;

				do {
					results = new BSONDocuments(transaction.readCursor(
							cursorId, MongoWP.MONGO_CURSOR_LIMIT));
					count += results.size();
				} while (results.size() >= MongoWP.MONGO_CURSOR_LIMIT);

				keyValues.put("n", count);
			} finally {
				transaction.closeCursor(cursorId);
			}
		} finally {
			transaction.close();
		}

		keyValues.put("ok", MongoWP.OK);
		BSONDocument reply = new MongoBSONDocument(keyValues);
		messageReplier.replyMessageNoCursor(reply);
	}

	@Override
	public void insert(BSONDocument document, MessageReplier messageReplier)
			throws Exception {
		AttributeMap attributeMap = messageReplier.getAttributeMap();
		ToroConnection connection = attributeMap.attr(
				ToroRequestProcessor.CONNECTION).get();

		Map<String, Object> keyValues = new HashMap<String, Object>();

		String collection = ToroCollectionTranslator
				.translate((String) document.getValue("insert"));
		WriteFailMode writeFailMode = getWriteFailMode(document);
		WriteConcern writeConcern = getWriteConcern(document);
		Iterable<?> documents = (Iterable<?>) document.getValue("documents");
		List<ToroDocument> inserts = new ArrayList<ToroDocument>();
		Iterator<?> documentsIterator = documents.iterator();
		while (documentsIterator.hasNext()) {
			BSONObject object = (BSONObject) documentsIterator.next();
			inserts.add(new BSONToroDocument(object));
		}

		ToroTransaction transaction = connection.createTransaction();

		try {
			Future<InsertResponse> futureInsertResponse = transaction
					.insertDocuments(collection, inserts, writeFailMode);

			Future<?> futureCommitResponse = transaction.commit();

			if (writeConcern.getW() > 0) {
				InsertResponse insertResponse = futureInsertResponse.get();
				futureCommitResponse.get();
				keyValues.put("n", insertResponse.getInsertedSize());
			}
			LastError lastError = new ToroLastError(RequestOpCode.OP_QUERY,
					QueryAndWriteOperationsQueryCommand.delete,
					futureInsertResponse, futureCommitResponse, false, null);
			attributeMap.attr(ToroRequestProcessor.LAST_ERROR).set(lastError);
		} finally {
			transaction.close();
		}

		keyValues.put("ok", MongoWP.OK);
		BSONDocument reply = new MongoBSONDocument(keyValues);
		messageReplier.replyMessageNoCursor(reply);
	}

	@Override
	public void update(BSONDocument document, MessageReplier messageReplier)
			throws Exception {
		AttributeMap attributeMap = messageReplier.getAttributeMap();
		ToroConnection connection = attributeMap.attr(
				ToroRequestProcessor.CONNECTION).get();

		Map<String, Object> keyValues = new HashMap<String, Object>();

		String collection = ToroCollectionTranslator
				.translate((String) document.getValue("update"));

		WriteFailMode writeFailMode = getWriteFailMode(document);
		WriteConcern writeConcern = getWriteConcern(document);
		QueryCriteriaTranslator queryCriteriaTranslator = new QueryCriteriaTranslator();
		Iterable<?> documents = (Iterable<?>) document.getValue("updates");
		List<UpdateOperation> updates = new ArrayList<UpdateOperation>();
		Iterator<?> documentsIterator = documents.iterator();
		while (documentsIterator.hasNext()) {
			BSONObject object = (BSONObject) documentsIterator.next();
			QueryCriteria queryCriteria = queryCriteriaTranslator
					.translate((BSONObject) object.get("q"));
			UpdateAction updateAction = UpdateActionTranslator
					.translate((BSONObject) object.get("u"));
			boolean upsert = getBoolean(object, "upsert", false);
			boolean onlyOne = !getBoolean(object, "multi", false);
			updates.add(new UpdateOperation(queryCriteria, updateAction,
					upsert, onlyOne));
		}

		ToroTransaction transaction = connection.createTransaction();

		try {
			Future<UpdateResponse> futureUpdateResponse = transaction.update(
					collection, updates, writeFailMode);

			Future<?> futureCommitResponse = transaction.commit();

			if (writeConcern.getW() > 0) {
				UpdateResponse updateResponse = futureUpdateResponse.get();
				futureCommitResponse.get();
				keyValues.put("n", updateResponse.getModified());
			}
			LastError lastError = new ToroLastError(RequestOpCode.OP_QUERY,
					QueryAndWriteOperationsQueryCommand.update,
					futureUpdateResponse, futureCommitResponse, false, null);
			attributeMap.attr(ToroRequestProcessor.LAST_ERROR).set(lastError);
		} finally {
			transaction.close();
		}

		keyValues.put("ok", MongoWP.OK);
		BSONDocument reply = new MongoBSONDocument(keyValues);
		messageReplier.replyMessageNoCursor(reply);
	}

	private boolean getBoolean(BSONDocument document, String key,
			boolean defaultValue) {
		if (!document.hasKey(key)) {
			return defaultValue;
		}

		Object value = document.getValue(key);

		if (value instanceof Boolean) {
			return ((Boolean) value).booleanValue();
		}

		if (value instanceof Number) {
			return ((Number) value).intValue() > 0;
		}

		throw new IllegalArgumentException("Value " + value + " for key " + key
				+ " is not boolean");
	}

	private boolean getBoolean(BSONObject object, String key,
			boolean defaultValue) {
		if (!object.containsField(key)) {
			return defaultValue;
		}

		Object value = object.get(key);

		if (value instanceof Boolean) {
			return ((Boolean) value).booleanValue();
		}

		if (value instanceof Number) {
			return ((Number) value).intValue() > 0;
		}

		throw new IllegalArgumentException("Value " + value + " for key " + key
				+ " is not boolean");
	}

	@Override
	public void delete(BSONDocument document, MessageReplier messageReplier)
			throws Exception {
		AttributeMap attributeMap = messageReplier.getAttributeMap();
		ToroConnection connection = attributeMap.attr(
				ToroRequestProcessor.CONNECTION).get();

		Map<String, Object> keyValues = new HashMap<String, Object>();

		String collection = ToroCollectionTranslator
				.translate((String) document.getValue("delete"));

		WriteFailMode writeFailMode = getWriteFailMode(document);
		WriteConcern writeConcern = getWriteConcern(document);
		QueryCriteriaTranslator queryCriteriaTranslator = new QueryCriteriaTranslator();
		Iterable<?> documents = (Iterable<?>) document.getValue("deletes");
		List<DeleteOperation> deletes = new ArrayList<DeleteOperation>();
		Iterator<?> documentsIterator = documents.iterator();
		while (documentsIterator.hasNext()) {
			BSONObject object = (BSONObject) documentsIterator.next();
			QueryCriteria queryCriteria = queryCriteriaTranslator
					.translate((BSONObject) object.get("q"));
			boolean singleRemove = getBoolean(object, "limit", false);
			deletes.add(new DeleteOperation(queryCriteria, singleRemove));
		}

		ToroTransaction transaction = connection.createTransaction();

		try {
			Future<DeleteResponse> futureDeleteResponse = transaction.delete(
					collection, deletes, writeFailMode);

			Future<?> futureCommitResponse = transaction.commit();

			if (writeConcern.getW() > 0) {
				DeleteResponse deleteResponse = futureDeleteResponse.get();
				futureCommitResponse.get();
				keyValues.put("n", deleteResponse.getDeleted());
			}
			LastError lastError = new ToroLastError(RequestOpCode.OP_QUERY,
					QueryAndWriteOperationsQueryCommand.delete,
					futureDeleteResponse, futureCommitResponse, false, null);
			attributeMap.attr(ToroRequestProcessor.LAST_ERROR).set(lastError);
		} finally {
			transaction.close();
		}

		keyValues.put("ok", MongoWP.OK);
		BSONDocument reply = new MongoBSONDocument(keyValues);
		messageReplier.replyMessageNoCursor(reply);
	}

	// TODO: implement with toro natives
	@Override
	public void drop(BSONDocument document, MessageReplier messageReplier)
			throws Exception {
		AttributeMap attributeMap = messageReplier.getAttributeMap();
		ToroConnection connection = attributeMap.attr(
				ToroRequestProcessor.CONNECTION).get();

		Map<String, Object> keyValues = new HashMap<String, Object>();

		String collection = ToroCollectionTranslator
				.translate((String) document.getValue("drop"));

		WriteFailMode writeFailMode = getWriteFailMode(document);
		List<DeleteOperation> deletes = new ArrayList<DeleteOperation>();
		QueryCriteria queryCriteria = TrueQueryCriteria.getInstance();
		deletes.add(new DeleteOperation(queryCriteria, false));

		ToroTransaction transaction = connection.createTransaction();

		try {
			Future<DeleteResponse> futureDropResponse = transaction.delete(
					collection, deletes, writeFailMode);

			Future<?> futureCommitResponse = transaction.commit();

			futureDropResponse.get();
			futureCommitResponse.get();

			LastError lastError = new ToroLastError(RequestOpCode.OP_QUERY,
					AdministrationQueryCommand.drop, futureDropResponse,
					futureCommitResponse, false, null);
			attributeMap.attr(ToroRequestProcessor.LAST_ERROR).set(lastError);
		} finally {
			transaction.close();
		}

		keyValues.put("ok", MongoWP.OK);
		BSONDocument reply = new MongoBSONDocument(keyValues);
		messageReplier.replyMessageNoCursor(reply);
	}

	public static final Map<String, Integer> NUM_INDEXES_MAP = new HashMap<String, Integer>();

	// TODO: implement with toro natives
	@Override
	public void createIndexes(@Nonnull BSONDocument document,
			@Nonnull MessageReplier messageReplier) throws Exception {
		Map<String, Object> keyValues = new HashMap<String, Object>();

		String collection = ToroCollectionTranslator
				.translate((String) document.getValue("createIndexes"));
		Iterable<?> indexes = (Iterable<?>) document.getValue("indexes");
		Iterator<?> indexesIterator = indexes.iterator();
		int newIndexesCount = 0;
		while (indexesIterator.hasNext()) {
			indexesIterator.next();
			newIndexesCount++;
		}

		keyValues.put("createdCollectionAutomatically", false);
		synchronized (NUM_INDEXES_MAP) {
			if (!NUM_INDEXES_MAP.containsKey(collection)) {
				NUM_INDEXES_MAP.put(collection, 0);
			}

			int numIndexes = NUM_INDEXES_MAP.get(collection);
			keyValues.put("numIndexesBefore", numIndexes);
			numIndexes += newIndexesCount;
			NUM_INDEXES_MAP.put(collection, numIndexes);
			keyValues.put("numIndexesAfter", numIndexes);
		}

		keyValues.put("ok", MongoWP.OK);
		BSONDocument reply = new MongoBSONDocument(keyValues);
		messageReplier.replyMessageNoCursor(reply);
	}

	private WriteFailMode getWriteFailMode(BSONDocument document) {
		return WriteFailMode.TRANSACTIONAL;
	}

	private WriteConcern getWriteConcern(BSONDocument document) {
		WriteConcern writeConcern = WriteConcern.ACKNOWLEDGED;
		if (document.hasKey("writeConcern")) {
			BSONObject writeConcernObject = (BSONObject) document
					.getValue("writeConcern");
			Object w = writeConcernObject.get("w");
			int wtimeout = 0;
			boolean fsync = false;
			boolean j = false;
			boolean continueOnError = false;
			Object jObject = writeConcernObject.get("j");
			if (jObject != null && jObject instanceof Boolean
					&& (Boolean) jObject) {
				fsync = true;
				j = true;
				continueOnError = true;
			}
			Object wtimeoutObject = writeConcernObject.get("wtimneout");
			if (wtimeoutObject != null && wtimeoutObject instanceof Number) {
				wtimeout = ((Number) wtimeoutObject).intValue();
			}
			if (w != null) {
				if (w instanceof Number) {
					if (((Number) w).intValue() <= 1 && wtimeout > 0) {
						throw new IllegalArgumentException(
								"wtimeout cannot be grater than 0 for w <= 1");
					}

					writeConcern = new WriteConcern(((Number) w).intValue(),
							wtimeout, fsync, j, continueOnError);
				} else if (w instanceof String && w.equals("majority")) {
					if (wtimeout > 0) {
						throw new IllegalArgumentException(
								"wtimeout cannot be grater than 0 for w <= 1");
					}

					writeConcern = new WriteConcern.Majority(wtimeout, fsync, j);
				} else {
					throw new IllegalArgumentException("w:" + w
							+ " is not supported");
				}
			}
		}
		return writeConcern;
	}

	@Override
	public void getLastError(Object w, boolean j, boolean fsync, int wtimeout,
			MessageReplier messageReplier) throws Exception {
		AttributeMap attributeMap = messageReplier.getAttributeMap();
		LastError lastError = attributeMap
				.attr(ToroRequestProcessor.LAST_ERROR).get();
		lastError.getLastError(w, j, fsync, wtimeout, messageReplier);
	}

	@Override
	public void validate(String database, BSONDocument document,
			MessageReplier messageReplier) {
		Map<String, Object> keyValues = new HashMap<String, Object>();

		String collection = ToroCollectionTranslator
				.translate((String) document.getValue("validate"));
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
		keyValues.put("firstExtentDetails", new BasicBSONObject(
				firstExtentDetailsKeyValues));

		keyValues.put("deletedCount", 0);
		keyValues.put("deletedSize", 0);
		keyValues.put("nIndexes", 1);

		Map<String, Object> keysPerIndexKeyValues = new HashMap<String, Object>();
		keysPerIndexKeyValues.put(ns + ".$_id_", 0);
		keyValues.put("keysPerIndex",
				new BasicBSONObject(keysPerIndexKeyValues));

		keyValues.put("valid", true);
		keyValues.put("errors", new String[0]);
		if (!full) {
			keyValues
					.put("warning",
							"Some checks omitted for speed. use {full:true} option to do more thorough scan.");
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

		keyValues.put("version", MongoWP.VERSION_STRING);
		keyValues.put("gitVersion", 41);
		keyValues.put("OpenSSLVersion", "");
		keyValues.put(
				"sysInfo",
				System.getProperty("os.name") + " "
						+ System.getProperty("os.version"));
		keyValues.put("loaderFlags", "");
		keyValues.put("compilerFlags", "");
		keyValues.put("allocator", "");
		keyValues.put("versionArray", MongoWP.VERSION);
		keyValues.put("javascriptEngine", "");
		keyValues.put("bits", 64);
		keyValues.put("debug", false);
		keyValues.put("maxBsonObjectSize", MongoWP.MAX_BSON_DOCUMENT_SIZE);
		keyValues.put("ok", MongoWP.OK);

		BSONDocument document = new MongoBSONDocument(keyValues);
		messageReplier.replyMessageNoCursor(document);
	}

	@Override
	public void create(BSONDocument document, MessageReplier messageReplier)
			throws Exception {
		BSONDocument reply = null;
		String collection = ToroCollectionTranslator
				.translate((String) document.getValue("create"));
		Boolean capped = (Boolean) document.getValue("capped");
		Boolean autoIndexId = (Boolean) document.getValue("autoIndexId");
		Boolean usePowerOf2Sizes = (Boolean) document.getValue("usePowerOf2Sizes");
		Double size = (Double) document.getValue("size");
		Double max = (Double) document.getValue("max");

		if (null != capped && true == capped) { // Other flags silently ignored
			ErrorCode errorCode = ErrorCode.UNIMPLEMENTED_FLAG;
			messageReplier.replyQueryCommandFailure(errorCode, "capped");
			return;
		}

		AttributeMap attributeMap = messageReplier.getAttributeMap();
		ToroConnection connection = attributeMap.attr(
				ToroRequestProcessor.CONNECTION).get();

		ToroTransaction transaction = connection.createTransaction();

		try {
			transaction.createEmptyCollection(collection);

			// Neccessary? Because DDL usually does not need a COMMIT anyway
			Future<?> futureCommitResponse = transaction.commit();
		} finally {
			transaction.close();
		}

		Map<String, Object> keyValues = new HashMap<String, Object>();
		keyValues.put("ok", MongoWP.OK);
		reply = new MongoBSONDocument(keyValues);
		messageReplier.replyMessageNoCursor(reply);
	}

	@Override
	public boolean handleError(QueryCommand userCommand,
			MessageReplier messageReplier, Throwable throwable)
			throws Exception {
		// TODO: Map real mongo error codes
		ErrorCode errorCode = ErrorCode.INTERNAL_ERROR;
		AttributeMap attributeMap = messageReplier.getAttributeMap();
		LastError lastError = new ToroLastError(RequestOpCode.OP_QUERY,
				userCommand, null, null, true, errorCode);
		attributeMap.attr(ToroRequestProcessor.LAST_ERROR).set(lastError);
		messageReplier.replyQueryCommandFailure(errorCode,
				throwable.getMessage());

		return true;
	}

	@Override
	public void unimplemented(QueryCommand userCommand,
			MessageReplier messageReplier) throws Exception {
		// TODO: Map real mongo error codes
		ErrorCode errorCode = ErrorCode.UNIMPLEMENTED_COMMAND;
		AttributeMap attributeMap = messageReplier.getAttributeMap();
		LastError lastError = new ToroLastError(RequestOpCode.OP_QUERY,
				userCommand, null, null, true, errorCode);
		attributeMap.attr(ToroRequestProcessor.LAST_ERROR).set(lastError);
		messageReplier
				.replyQueryCommandFailure(errorCode, userCommand.getKey());
	}
}
