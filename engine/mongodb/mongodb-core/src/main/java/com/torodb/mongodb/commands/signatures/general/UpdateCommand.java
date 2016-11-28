/*
 * ToroDB
 * Copyright © 2014 8Kdata Technology (www.8kdata.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.torodb.mongodb.commands.signatures.general;

import com.eightkdata.mongowp.MongoConstants;
import com.eightkdata.mongowp.WriteConcern;
import com.eightkdata.mongowp.bson.BsonDocument;
import com.eightkdata.mongowp.bson.BsonValue;
import com.eightkdata.mongowp.exceptions.BadValueException;
import com.eightkdata.mongowp.exceptions.FailedToParseException;
import com.eightkdata.mongowp.exceptions.MongoException;
import com.eightkdata.mongowp.exceptions.NoSuchKeyException;
import com.eightkdata.mongowp.exceptions.TypesMismatchException;
import com.eightkdata.mongowp.fields.ArrayField;
import com.eightkdata.mongowp.fields.BooleanField;
import com.eightkdata.mongowp.fields.DocField;
import com.eightkdata.mongowp.fields.DoubleField;
import com.eightkdata.mongowp.fields.IntField;
import com.eightkdata.mongowp.fields.LongField;
import com.eightkdata.mongowp.fields.StringField;
import com.eightkdata.mongowp.server.api.MarshalException;
import com.eightkdata.mongowp.server.api.impl.AbstractNotAliasableCommand;
import com.eightkdata.mongowp.utils.BsonArrayBuilder;
import com.eightkdata.mongowp.utils.BsonDocumentBuilder;
import com.eightkdata.mongowp.utils.BsonReaderTool;
import com.google.common.collect.ImmutableList;
import com.torodb.mongodb.commands.pojos.WriteConcernError;
import com.torodb.mongodb.commands.pojos.WriteError;
import com.torodb.mongodb.commands.signatures.general.UpdateCommand.UpdateArgument;
import com.torodb.mongodb.commands.signatures.general.UpdateCommand.UpdateResult;

import javax.annotation.Nullable;

public class UpdateCommand extends AbstractNotAliasableCommand<UpdateArgument, UpdateResult> {

  public static final UpdateCommand INSTANCE = new UpdateCommand();
  private static final String COMMAND_NAME = "update";

  private UpdateCommand() {
    super(COMMAND_NAME);
  }

  @Override
  public Class<? extends UpdateArgument> getArgClass() {
    return UpdateArgument.class;
  }

  @Override
  public UpdateArgument unmarshallArg(BsonDocument requestDoc) throws
      BadValueException, TypesMismatchException, NoSuchKeyException,
      FailedToParseException {
    return UpdateArgument.unmarshall(requestDoc);
  }

  @Override
  public BsonDocument marshallArg(UpdateArgument request) throws
      MarshalException {
    return request.marshall();
  }

  @Override
  public Class<? extends UpdateResult> getResultClass() {
    return UpdateResult.class;
  }

  @Override
  public UpdateResult unmarshallResult(BsonDocument resultDoc) throws
      BadValueException, TypesMismatchException, NoSuchKeyException,
      FailedToParseException, MongoException {
    return UpdateResult.unmarshall(resultDoc);
  }

  @Override
  public BsonDocument marshallResult(UpdateResult result) throws
      MarshalException {
    return result.marshall();
  }

  public static class UpdateArgument {

    private static final StringField COLLECTION_FIELD = new StringField(COMMAND_NAME);
    private static final ArrayField UPDATES_FIELD = new ArrayField("updates");
    private static final BooleanField ORDERED_FIELD = new BooleanField("ordered");
    private static final DocField WRITE_CONCERN_FIELD = new DocField("writeConcern");

    private final String collection;
    private final Iterable<UpdateStatement> statements;
    private final boolean ordered;
    private final WriteConcern writeConcern;

    public UpdateArgument(String collection, Iterable<UpdateStatement> statements, boolean ordered,
        WriteConcern writeConcern) {
      this.collection = collection;
      this.statements = statements;
      this.ordered = ordered;
      this.writeConcern = writeConcern;
    }

    private BsonDocument marshall() {
      BsonArrayBuilder updatesArray = new BsonArrayBuilder();
      for (UpdateStatement update : statements) {
        updatesArray.add(update.marshall());
      }

      return new BsonDocumentBuilder()
          .append(COLLECTION_FIELD, collection)
          .append(UPDATES_FIELD, updatesArray.build())
          .append(ORDERED_FIELD, ordered)
          .append(WRITE_CONCERN_FIELD, writeConcern.toDocument())
          .build();
    }

    private static UpdateArgument unmarshall(BsonDocument requestDoc) throws TypesMismatchException,
        NoSuchKeyException, FailedToParseException, BadValueException {
      ImmutableList.Builder<UpdateStatement> updates = ImmutableList.builder();

      for (BsonValue element : BsonReaderTool.getArray(requestDoc, UPDATES_FIELD)) {
        if (!element.isDocument()) {
          throw new BadValueException(UPDATES_FIELD.getFieldName()
              + " array contains the unexpected value '"
              + element + "' which is not a document");
        }
        updates.add(UpdateStatement.unmarshall(element.asDocument()));
      }

      WriteConcern writeConcern = WriteConcern.fromDocument(
          BsonReaderTool.getDocument(
              requestDoc,
              WRITE_CONCERN_FIELD,
              null
          ),
          WriteConcern.acknowledged() //TODO: Check the default WC
      );

      return new UpdateArgument(
          BsonReaderTool.getString(requestDoc, COLLECTION_FIELD),
          updates.build(),
          BsonReaderTool.getBoolean(requestDoc, ORDERED_FIELD),
          writeConcern
      );
    }

    public String getCollection() {
      return collection;
    }

    public Iterable<UpdateStatement> getStatements() {
      return statements;
    }

    public boolean isOrdered() {
      return ordered;
    }

    public WriteConcern getWriteConcern() {
      return writeConcern;
    }

  }

  public static class UpdateStatement {

    private static final DocField QUERY_FIELD = new DocField("q");
    private static final DocField UPDATE_FIELD = new DocField("u");
    private static final BooleanField UPSERT_FIELD = new BooleanField("upsert");
    private static final BooleanField MULTI_FIELD = new BooleanField("multi");

    private final BsonDocument query;
    private final BsonDocument update;
    private final boolean upsert;
    private final boolean multi;

    public UpdateStatement(BsonDocument query, BsonDocument update, boolean upsert, boolean multi) {
      this.query = query;
      this.update = update;
      this.upsert = upsert;
      this.multi = multi;
    }

    private static UpdateStatement unmarshall(BsonDocument element) throws TypesMismatchException,
        NoSuchKeyException {
      return new UpdateStatement(
          BsonReaderTool.getDocument(element, QUERY_FIELD),
          BsonReaderTool.getDocument(element, UPDATE_FIELD),
          BsonReaderTool.getBoolean(element, UPSERT_FIELD, false),
          BsonReaderTool.getBoolean(element, MULTI_FIELD, false)
      );
    }

    private BsonDocument marshall() {
      return new BsonDocumentBuilder()
          .append(QUERY_FIELD, query)
          .append(UPDATE_FIELD, update)
          .append(UPSERT_FIELD, upsert)
          .append(MULTI_FIELD, multi)
          .build();
    }

    public BsonDocument getQuery() {
      return query;
    }

    public BsonDocument getUpdate() {
      return update;
    }

    public boolean isUpsert() {
      return upsert;
    }

    public boolean isMulti() {
      return multi;
    }
  }

  public static class UpdateResult {

    private static final LongField MODIFIED_COUNTER_FIELD = new LongField("nModified");
    private static final LongField CANDIDATE_COUNTER_FIELD = new LongField("n");
    private static final ArrayField WRITE_ERRORS_FIELD = new ArrayField("writeErrors");
    @SuppressWarnings("checkstyle:LineLength")
    private static final ArrayField WRITE_CONCERN_ERRORS_FIELD = new ArrayField("writeConcernError");
    private static final ArrayField UPSERTED_ARRAY = new ArrayField("upserted");
    private static final StringField ERR_MSG_FIELD = new StringField("errmsg");
    private static final DoubleField OK_FIELD = new DoubleField("ok");

    private final long modifiedCounter;
    private final long candidateCounter;
    @Nullable
    private final String errorMessage;
    private final ImmutableList<WriteError> writeErrors;
    private final ImmutableList<WriteConcernError> writeConcernErrors;
    private final ImmutableList<UpsertResult> upserts;

    public UpdateResult(long modifiedCounter, long candidateCounter) {
      this.modifiedCounter = modifiedCounter;
      this.candidateCounter = candidateCounter;
      this.errorMessage = null;
      this.writeErrors = ImmutableList.of();
      this.writeConcernErrors = ImmutableList.of();
      this.upserts = ImmutableList.of();
    }

    public UpdateResult(long modifiedCounter, long candidateCounter,
        ImmutableList<UpsertResult> upserts) {
      this.modifiedCounter = modifiedCounter;
      this.candidateCounter = candidateCounter;
      this.upserts = upserts;
      this.errorMessage = null;
      this.writeErrors = ImmutableList.of();
      this.writeConcernErrors = ImmutableList.of();
    }

    public UpdateResult(
        long modifiedCounter,
        long candidateCounter,
        String errorMessage,
        ImmutableList<WriteError> writeErrors,
        ImmutableList<WriteConcernError> writeConcernErrors) {
      this.modifiedCounter = modifiedCounter;
      this.candidateCounter = candidateCounter;
      this.errorMessage = errorMessage;
      this.writeErrors = writeErrors;
      this.writeConcernErrors = writeConcernErrors;
      this.upserts = ImmutableList.of();
    }

    public UpdateResult(
        long modifiedCounter,
        long candidateCounter,
        ImmutableList<UpsertResult> upserts,
        String errorMessage,
        ImmutableList<WriteError> writeErrors,
        ImmutableList<WriteConcernError> writeConcernErrors) {
      this.modifiedCounter = modifiedCounter;
      this.candidateCounter = candidateCounter;
      this.errorMessage = errorMessage;
      this.writeErrors = writeErrors;
      this.writeConcernErrors = writeConcernErrors;
      this.upserts = upserts;
    }

    private static UpdateResult unmarshall(BsonDocument resultDoc) throws TypesMismatchException,
        NoSuchKeyException, BadValueException {
      boolean ok = BsonReaderTool.getNumeric(resultDoc, OK_FIELD).asNumber().longValue() != 0;
      long modified = BsonReaderTool.getNumeric(resultDoc, MODIFIED_COUNTER_FIELD).asNumber()
          .longValue();
      long candidates = BsonReaderTool.getNumeric(resultDoc, CANDIDATE_COUNTER_FIELD).asNumber()
          .longValue();

      if (ok) {
        return new UpdateResult(modified, candidates);
      } else {
        ImmutableList.Builder<WriteError> writeErrors = ImmutableList.builder();
        if (BsonReaderTool.containsField(resultDoc, WRITE_ERRORS_FIELD)) {
          for (BsonValue element : BsonReaderTool.getArray(resultDoc, WRITE_ERRORS_FIELD)) {
            if (!element.isDocument()) {
              throw new BadValueException(WRITE_ERRORS_FIELD.getFieldName()
                  + " array contains the unexpected value '"
                  + element + "' which is not a document");
            }
            writeErrors.add(WriteError.unmarshall(element.asDocument()));
          }
        }

        ImmutableList.Builder<WriteConcernError> writeConcernErrors = ImmutableList.builder();
        if (BsonReaderTool.containsField(resultDoc, WRITE_CONCERN_ERRORS_FIELD)) {
          for (BsonValue element : BsonReaderTool.getArray(resultDoc, WRITE_CONCERN_ERRORS_FIELD)) {
            if (!element.isDocument()) {
              throw new BadValueException(WRITE_CONCERN_ERRORS_FIELD.getFieldName()
                  + " array contains the unexpected value '"
                  + element + "' which is not a document");
            }
            writeConcernErrors.add(WriteConcernError.unmarshall(element.asDocument()));
          }
        }

        ImmutableList.Builder<UpsertResult> upserts = ImmutableList.builder();
        if (BsonReaderTool.containsField(resultDoc, UPSERTED_ARRAY)) {
          for (BsonValue element : BsonReaderTool.getArray(resultDoc, UPSERTED_ARRAY)) {
            if (!element.isDocument()) {
              throw new BadValueException(UPSERTED_ARRAY.getFieldName()
                  + " array contains the unexpected value '"
                  + element + "' which is not a document");
            }
            upserts.add(UpsertResult.unmarshall(element.asDocument()));
          }
        }

        return new UpdateResult(
            modified,
            candidates,
            upserts.build(),
            BsonReaderTool.getString(resultDoc, ERR_MSG_FIELD, null),
            writeErrors.build(),
            writeConcernErrors.build()
        );
      }
    }

    private BsonDocument marshall() {
      BsonDocumentBuilder builder = new BsonDocumentBuilder()
          .append(OK_FIELD, isOk() ? MongoConstants.OK : MongoConstants.KO)
          .appendNumber(MODIFIED_COUNTER_FIELD, modifiedCounter)
          .appendNumber(CANDIDATE_COUNTER_FIELD, candidateCounter);

      if (!writeErrors.isEmpty()) {
        BsonArrayBuilder array = new BsonArrayBuilder();
        for (WriteError writeError : writeErrors) {
          array.add(writeError.marshall());
        }
        builder.append(WRITE_ERRORS_FIELD, array.build());
      }

      if (!writeConcernErrors.isEmpty()) {
        BsonArrayBuilder array = new BsonArrayBuilder();
        for (WriteConcernError writeConcernError : writeConcernErrors) {
          array.add(writeConcernError.marshall());
        }
        builder.append(WRITE_CONCERN_ERRORS_FIELD, array.build());
      }
      if (!upserts.isEmpty()) {
        BsonArrayBuilder array = new BsonArrayBuilder();
        for (UpsertResult upsert : upserts) {
          array.add(upsert.marshall());
        }
        builder.append(UPSERTED_ARRAY, array.build());
      }
      if (errorMessage != null) {
        builder.append(ERR_MSG_FIELD, errorMessage);
      }
      return builder.build();
    }

    public boolean isOk() {
      return writeConcernErrors.isEmpty() && writeErrors.isEmpty();
    }

    public long getModifiedCounter() {
      return modifiedCounter;
    }

    public long getCandidateCounter() {
      return candidateCounter;
    }

    public String getErrorMessage() {
      return errorMessage;
    }

    public ImmutableList<WriteError> getWriteErrors() {
      return writeErrors;
    }

    public ImmutableList<WriteConcernError> getWriteConcernErrors() {
      return writeConcernErrors;
    }

    public ImmutableList<UpsertResult> getUpserts() {
      return upserts;
    }
  }

  public static class UpsertResult {

    private static final IntField INDEX_FIELD = new IntField("index");
    private static final String ID_FIELD_ID = "_id";

    private final int index;
    private final BsonValue<?> id;

    public UpsertResult(int index, BsonValue<?> id) {
      this.index = index;
      this.id = id;
    }

    private static UpsertResult unmarshall(BsonDocument document) throws TypesMismatchException,
        NoSuchKeyException {
      return new UpsertResult(
          BsonReaderTool.getInteger(document, INDEX_FIELD),
          BsonReaderTool.getValue(document, ID_FIELD_ID)
      );
    }

    private BsonDocument marshall() {
      return new BsonDocumentBuilder()
          .append(INDEX_FIELD, index)
          .appendUnsafe(ID_FIELD_ID, id)
          .build();
    }

    public int getIndex() {
      return index;
    }

    public BsonValue getId() {
      return id;
    }
  }

}
