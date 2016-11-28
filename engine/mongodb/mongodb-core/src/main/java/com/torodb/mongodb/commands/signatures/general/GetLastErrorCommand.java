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

import com.eightkdata.mongowp.ErrorCode;
import com.eightkdata.mongowp.MongoConstants;
import com.eightkdata.mongowp.OpTime;
import com.eightkdata.mongowp.WriteConcern;
import com.eightkdata.mongowp.WriteConcern.SyncMode;
import com.eightkdata.mongowp.bson.BsonDocument;
import com.eightkdata.mongowp.bson.BsonDocument.Entry;
import com.eightkdata.mongowp.bson.BsonObjectId;
import com.eightkdata.mongowp.bson.BsonTimestamp;
import com.eightkdata.mongowp.exceptions.BadValueException;
import com.eightkdata.mongowp.exceptions.FailedToParseException;
import com.eightkdata.mongowp.exceptions.NoSuchKeyException;
import com.eightkdata.mongowp.exceptions.TypesMismatchException;
import com.eightkdata.mongowp.fields.ArrayField;
import com.eightkdata.mongowp.fields.BooleanField;
import com.eightkdata.mongowp.fields.DocField;
import com.eightkdata.mongowp.fields.DoubleField;
import com.eightkdata.mongowp.fields.IntField;
import com.eightkdata.mongowp.fields.ObjectIdField;
import com.eightkdata.mongowp.fields.StringField;
import com.eightkdata.mongowp.fields.TimestampField;
import com.eightkdata.mongowp.server.api.impl.AbstractNotAliasableCommand;
import com.eightkdata.mongowp.server.callback.WriteOpResult;
import com.eightkdata.mongowp.utils.BsonArrayBuilder;
import com.eightkdata.mongowp.utils.BsonDocumentBuilder;
import com.eightkdata.mongowp.utils.BsonReaderTool;
import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;
import com.torodb.mongodb.commands.signatures.general.GetLastErrorCommand.GetLastErrorArgument;
import com.torodb.mongodb.commands.signatures.general.GetLastErrorCommand.GetLastErrorReply;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

public class GetLastErrorCommand
    extends AbstractNotAliasableCommand<GetLastErrorArgument, GetLastErrorReply> {

  public static final GetLastErrorCommand INSTANCE = new GetLastErrorCommand();

  private GetLastErrorCommand() {
    super("getLastError");
  }

  @Override
  public Class<? extends GetLastErrorArgument> getArgClass() {
    return GetLastErrorArgument.class;
  }

  @Override
  public GetLastErrorArgument unmarshallArg(BsonDocument requestDoc)
      throws TypesMismatchException, NoSuchKeyException, BadValueException {
    return GetLastErrorArgument.unmarshall(requestDoc);
  }

  @Override
  public BsonDocument marshallArg(GetLastErrorArgument request) {
    throw new UnsupportedOperationException("Not supported yet."); //TODO
  }

  @Override
  public Class<? extends GetLastErrorReply> getResultClass() {
    return GetLastErrorReply.class;
  }

  @Override
  public BsonDocument marshallResult(GetLastErrorReply reply) {
    return reply.marshall();
  }

  @Override
  public GetLastErrorReply unmarshallResult(BsonDocument resultDoc) {
    throw new UnsupportedOperationException("Not supported yet."); //TODO
  }

  @SuppressWarnings("checkstyle:AbbreviationAsWordInName")
  public static class GetLastErrorArgument {

    private static final TimestampField W_OP_TIME_FIELD = new TimestampField("wOpTime");
    private static final ObjectIdField W_ELECTION_ID_FIELD = new ObjectIdField("wElectionId");

    @Nullable
    private final WriteConcern writeConcern;
    @Nullable
    private final BsonDocument badGLE;
    @Nullable
    private final OpTime wOpTime;
    @Nullable
    private final BsonObjectId wElectionId;
    @Nullable
    private final String badGLEMessage;
    private final ErrorCode badGLEErrorCode;

    public GetLastErrorArgument(
        WriteConcern writeConcern,
        BsonDocument badGLE,
        String badGLEMessage,
        @Nonnull ErrorCode badGLEErrorCode,
        OpTime wOpTime,
        BsonObjectId wElectionId) {
      this.writeConcern = writeConcern;
      this.badGLE = badGLE;
      this.badGLEMessage = badGLEMessage;
      this.badGLEErrorCode = badGLEErrorCode;
      this.wOpTime = wOpTime;
      this.wElectionId = wElectionId;
    }

    public boolean isValid() {
      return badGLE == null;
    }

    public String getBadGLEMessage() {
      return badGLEMessage;
    }

    @Nonnull
    public ErrorCode getBadGLEErrorCode() {
      return badGLEErrorCode;
    }

    @Nullable
    public WriteConcern getWriteConcern() {
      return writeConcern;
    }

    @Nullable
    public BsonDocument getBadGLE() {
      return badGLE;
    }

    @Nullable
    public OpTime getWOpTime() {
      return wOpTime;
    }

    @Nullable
    public BsonObjectId getWElectionId() {
      return wElectionId;
    }

    private static GetLastErrorArgument unmarshall(BsonDocument requestDoc) {
      OpTime opTime;
      try {
        BsonTimestamp ts = BsonReaderTool.getTimestamp(
            requestDoc, W_OP_TIME_FIELD);
        opTime = new OpTime(ts);
      } catch (TypesMismatchException ex) {
        return new GetLastErrorArgument(
            null,
            requestDoc,
            ex.getLocalizedMessage(),
            ex.getErrorCode(),
            null,
            null
        );
      } catch (NoSuchKeyException ex) {
        opTime = null;
      }
      BsonObjectId electionId;
      try {
        electionId = BsonReaderTool.getObjectId(requestDoc, W_ELECTION_ID_FIELD, null);
      } catch (TypesMismatchException ex) {
        return new GetLastErrorArgument(
            null,
            requestDoc,
            ex.getLocalizedMessage(),
            ex.getErrorCode(),
            opTime,
            null
        );
      }
      WriteConcern wc;
      try {
        wc = WriteConcern.fromDocument(requestDoc);
      } catch (FailedToParseException ex) {
        return new GetLastErrorArgument(
            null,
            requestDoc,
            ex.getLocalizedMessage(),
            ex.getErrorCode(),
            opTime,
            electionId
        );
      }
      return new GetLastErrorArgument(
          wc,
          null,
          null,
          ErrorCode.OK,
          opTime,
          electionId
      );
    }

  }

  public static class GetLastErrorReply {

    private static final DoubleField OK_FIELD = new DoubleField("ok");
    private static final DoubleField CONNECTION_ID_FIELD = new DoubleField("connectionId");
    private static final DocField BAD_GLE_FIELD = new DocField("badGLE");
    private static final StringField ERR_MSG_FIELD = new StringField("errmsg");
    private static final StringField ERR_FIELD = new StringField("err");
    private static final IntField CODE_FIELD = new IntField("code");

    @Nonnull
    private final ErrorCode thisError;
    @Nullable
    private final String thisErrorMessage;
    private final long connectionId;
    @Nullable
    private final WriteOpResult writeOpResult;
    @Nullable
    private final GetLastErrorArgument arg;
    @Nullable
    private final WriteConcernEnforcementResult wcer;

    public GetLastErrorReply(
        @Nonnull ErrorCode thisError,
        @Nullable String thisErrorMessage,
        long connectionId,
        @Nullable WriteOpResult writeOpResult,
        @Nonnull GetLastErrorArgument arg,
        @Nullable WriteConcernEnforcementResult wcer) {
      this.thisError = thisError;
      this.thisErrorMessage = thisErrorMessage;
      this.connectionId = connectionId;
      this.writeOpResult = writeOpResult;
      this.arg = arg;
      this.wcer = wcer;
    }

    private BsonDocument marshall() {
      BsonDocumentBuilder builder = new BsonDocumentBuilder();
      builder.append(CONNECTION_ID_FIELD, connectionId);

      if (arg.getBadGLE() != null) {
        builder.append(BAD_GLE_FIELD, arg.getBadGLE());
        builder.append(ERR_MSG_FIELD, arg.getBadGLEMessage());
        return builder.build();
      }

      if (writeOpResult != null) {
        for (Entry<?> entry : writeOpResult.marshall()) {
          builder.appendUnsafe(entry.getKey(), entry.getValue());
        }

        if (writeOpResult.errorOcurred()) {
          return builder.build();
        }
      }

      if (wcer != null) {
        wcer.marshall(builder);
      }

      if (!thisError.equals(ErrorCode.OK)) {
        builder.append(CODE_FIELD, thisError.getErrorCode());
        if (thisErrorMessage != null) {
          builder.append(ERR_FIELD, thisErrorMessage);
        }
      }

      boolean ok = thisError.equals(ErrorCode.OK);
      builder.append(OK_FIELD, ok ? MongoConstants.OK : MongoConstants.KO);

      return builder.build();
    }
  }

  @Immutable
  public static class WriteConcernEnforcementResult {

    private static final IntField SYNC_MILLIS_FIELD = new IntField("syncMillis");
    private static final IntField FSYNC_FILES_FIELD = new IntField("fsyncFiles");
    private static final StringField ERR_FIELD = new StringField("err");
    private static final IntField W_TIMED_FIELD = new IntField("wtimeout");
    private static final IntField WAITED_FIELD = new IntField("waited");
    private static final BooleanField W_TIMEDOUT_FIELD = new BooleanField("wtimeout");
    private static final ArrayField WRITTEN_TO_FIELD = new ArrayField("writtenTo");

    @Nonnull
    private final WriteConcern writeConcern;
    @Nullable
    private final String err;
    @Nullable
    @Nonnegative
    private final Integer syncMillis;
    @Nullable
    @Nonnegative
    private final Integer fsyncFiles;
    private final boolean wTimedOut;
    @Nullable
    @Nonnegative
    private final Integer wTime;
    private final ImmutableList<HostAndPort> writtenTo;

    public WriteConcernEnforcementResult(
        @Nonnull WriteConcern writeConcern,
        @Nullable String err,
        @Nullable Integer syncMillis,
        @Nullable Integer fsyncFiles,
        boolean wTimedOut,
        @Nullable Integer wTime,
        @Nullable ImmutableList<HostAndPort> writtenTo) {
      this.writeConcern = writeConcern;
      this.err = err;
      this.syncMillis = syncMillis;
      this.fsyncFiles = fsyncFiles;
      this.wTimedOut = wTimedOut;
      this.wTime = wTime;
      this.writtenTo = writtenTo;
    }

    private void marshall(BsonDocumentBuilder builder) {
      if (syncMillis != null) {
        builder.append(SYNC_MILLIS_FIELD, syncMillis);
      }
      if (fsyncFiles != null) {
        builder.append(FSYNC_FILES_FIELD, fsyncFiles);
      }
      if (wTime != null) {
        if (wTimedOut) {
          builder.append(WAITED_FIELD, wTime);
        } else {
          builder.append(W_TIMED_FIELD, wTime);
        }
      }

      // *** 2.4 SyncClusterConnection compatibility ***
      // 2.4 expects either fsync'd files, or a "waited" field exist after running an fsync : true
      // GLE, but with journaling we don't actually need to run the fsync (fsync command is
      // preferred in 2.6).  So we add a "waited" field if one doesn't exist.
      if (writeConcern.getSyncMode() == SyncMode.FSYNC
          && !builder.containsField(WAITED_FIELD)
          && !builder.containsField(FSYNC_FILES_FIELD)) {
        if (wTime != null) {
          builder.append(WAITED_FIELD, wTime);
        } else {
          assert syncMillis != null;
          builder.append(WAITED_FIELD, syncMillis);
        }
      }

      if (wTimedOut) {
        builder.append(W_TIMEDOUT_FIELD, wTimedOut);
      }

      if (writtenTo != null && !writtenTo.isEmpty()) {
        BsonArrayBuilder array = new BsonArrayBuilder();
        for (HostAndPort w : writtenTo) {
          array.add(w.toString());
        }
        builder.append(WRITTEN_TO_FIELD, array.build());
      } else {
        builder.appendNull(WRITTEN_TO_FIELD);
      }

      if (err == null || err.isEmpty()) {
        builder.appendNull(ERR_FIELD);
      } else {
        builder.append(ERR_FIELD, err);
      }
    }
  }

}
