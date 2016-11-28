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

package com.torodb.mongodb.commands.signatures.internal;

import static com.eightkdata.mongowp.bson.BsonType.DATETIME;
import static com.eightkdata.mongowp.bson.BsonType.DOCUMENT;
import static com.eightkdata.mongowp.bson.BsonType.TIMESTAMP;

import com.eightkdata.mongowp.ErrorCode;
import com.eightkdata.mongowp.MongoConstants;
import com.eightkdata.mongowp.OpTime;
import com.eightkdata.mongowp.bson.BsonDocument;
import com.eightkdata.mongowp.bson.BsonDocument.Entry;
import com.eightkdata.mongowp.bson.BsonNumber;
import com.eightkdata.mongowp.bson.BsonTimestamp;
import com.eightkdata.mongowp.bson.BsonType;
import com.eightkdata.mongowp.bson.BsonValue;
import com.eightkdata.mongowp.exceptions.BadValueException;
import com.eightkdata.mongowp.exceptions.FailedToParseException;
import com.eightkdata.mongowp.exceptions.InconsistentReplicaSetNamesException;
import com.eightkdata.mongowp.exceptions.MongoException;
import com.eightkdata.mongowp.exceptions.NoSuchKeyException;
import com.eightkdata.mongowp.exceptions.TypesMismatchException;
import com.eightkdata.mongowp.exceptions.UnknownErrorException;
import com.eightkdata.mongowp.fields.BooleanField;
import com.eightkdata.mongowp.fields.DateTimeField;
import com.eightkdata.mongowp.fields.DocField;
import com.eightkdata.mongowp.fields.DoubleField;
import com.eightkdata.mongowp.fields.HostAndPortField;
import com.eightkdata.mongowp.fields.IntField;
import com.eightkdata.mongowp.fields.LongField;
import com.eightkdata.mongowp.fields.StringField;
import com.eightkdata.mongowp.fields.TimestampField;
import com.eightkdata.mongowp.utils.BsonDocumentBuilder;
import com.eightkdata.mongowp.utils.BsonReaderTool;
import com.google.common.net.HostAndPort;
import com.torodb.mongodb.commands.pojos.MemberState;
import com.torodb.mongodb.commands.pojos.ReplicaSetConfig;

import java.time.Duration;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 *
 */
public class ReplSetHeartbeatReplyMarshaller {

  private static final DocField CONFIG_FIELD = new DocField("config");
  private static final LongField CONFIG_VERSION_FIELD = new LongField("v");
  private static final TimestampField ELECTION_TIME_FIELD = new TimestampField("electionTime");
  private static final StringField ERR_MSG_FIELD = new StringField("errmsg");
  private static final IntField ERROR_CODE_FIELD = new IntField("code");
  private static final BooleanField HAS_DATA_FIELD = new BooleanField("hasData");
  @SuppressWarnings("checkstyle:LineLength")
  private static final BooleanField HAS_STATE_DISAGREEMENT_FIELD = new BooleanField("stateDisagreement");
  private static final StringField HB_MESSAGE_FIELD = new StringField("hbmsg");
  private static final BooleanField IS_ELECTABLE_FIELD = new BooleanField("e");
  private static final BooleanField IS_REPL_SET_FIELD = new BooleanField("rs");
  private static final IntField MEMBER_STATE_FIELD = new IntField("state");
  private static final BooleanField MISMATCH_FIELD = new BooleanField("mismatch");
  private static final DoubleField OK_FIELD = new DoubleField("ok");
  private static final String APPLIED_OP_TIME_FIELD_NAME = "opTime";
  @SuppressWarnings("checkstyle:LineLength")
  private static final DocField APPLIED_OP_TIME_DOC_FIELD = new DocField(APPLIED_OP_TIME_FIELD_NAME);
  @SuppressWarnings("checkstyle:LineLength")
  private static final DateTimeField APPLIED_OP_TIME_DT_FIELD = new DateTimeField(APPLIED_OP_TIME_FIELD_NAME);
  private static final StringField REPL_SET_FIELD = new StringField("set");
  private static final HostAndPortField SYNC_SOURCE_FIELD = new HostAndPortField("syncingTo");
  private static final LongField TIME_FIELD = new LongField("time");
  private static final LongField TERM_FIELD = new LongField("term");
  private static final DocField DURABLE_OP_TIME_FIELD = new DocField("durableOpTime");
  private static final LongField PRIMARY_ID_FIELD = new LongField("primaryId");

  private ReplSetHeartbeatReplyMarshaller() {
  }

  static BsonDocument marshall(ReplSetHeartbeatReply reply, boolean asV1) {
    BsonDocumentBuilder doc = new BsonDocumentBuilder();
    if (reply.isMismatch()) {
      doc.append(OK_FIELD, MongoConstants.KO)
          .append(MISMATCH_FIELD, true);
      return doc.build();
    }
    if (reply.getErrorCode().isOk()) {
      doc.append(OK_FIELD, MongoConstants.OK);
    } else {
      doc.append(OK_FIELD, MongoConstants.KO);
      doc.append(ERROR_CODE_FIELD, reply.getErrorCode().getErrorCode());
      if (reply.getErrMsg().isPresent()) {
        doc.append(ERR_MSG_FIELD, reply.getErrMsg().get());
      }
    }
    reply.getTime().ifPresent(time ->
        doc.append(TIME_FIELD, time.getSeconds()));
    reply.getElectionTime().ifPresent(electionTime ->
        doc.append(ELECTION_TIME_FIELD, electionTime));
    reply.getConfig().ifPresent(config ->
        doc.append(CONFIG_FIELD, config.toBson()));
    reply.getElectable().ifPresent(electable ->
        doc.append(IS_ELECTABLE_FIELD, electable));
    reply.getIsReplSet().ifPresent(isRepSet ->
        doc.append(IS_REPL_SET_FIELD, isRepSet));
    doc.append(HAS_STATE_DISAGREEMENT_FIELD, reply.isStateDisagreement());
    reply.getState().ifPresent(state ->
        doc.append(MEMBER_STATE_FIELD, state.getId()));
    doc.append(CONFIG_VERSION_FIELD, reply.getConfigVersion());
    doc.append(HB_MESSAGE_FIELD, reply.getHbmsg());
    reply.getSetName().ifPresent(setName ->
        doc.append(REPL_SET_FIELD, setName));
    reply.getSyncingTo().ifPresent(syncingTo ->
        doc.append(SYNC_SOURCE_FIELD, syncingTo.toString()));
    reply.getHasData().ifPresent(hasData ->
        doc.append(HAS_DATA_FIELD, hasData));
    if (reply.getTerm() != -1) {
      doc.append(TERM_FIELD, reply.getTerm());
    }
    reply.getPrimaryId().ifPresent(primaryId ->
        doc.append(PRIMARY_ID_FIELD, primaryId));
    reply.getDurableOptime().ifPresent(durableOptime ->
        doc.append(DURABLE_OP_TIME_FIELD, durableOptime.toBson()));
    reply.getAppliedOpTime().ifPresent(applyied -> {
      if (asV1) {
        doc.append(APPLIED_OP_TIME_DOC_FIELD, applyied.toBson());
      } else {
        applyied.appendAsOldBson(doc, APPLIED_OP_TIME_FIELD_NAME);
      }
    });
    return doc.build();
  }

  static ReplSetHeartbeatReply unmarshall(BsonDocument bson) throws
      TypesMismatchException, NoSuchKeyException, BadValueException,
      FailedToParseException, MongoException {
    // Old versions set this even though they returned not "ok"
    boolean mismatch = BsonReaderTool.getBoolean(bson, MISMATCH_FIELD, false);
    if (mismatch) {
      throw new InconsistentReplicaSetNamesException();
    }

    ReplSetHeartbeatReplyBuilder builder = new ReplSetHeartbeatReplyBuilder();

    // Old versions sometimes set the replica set name ("set") but ok:0
    String setName;
    try {
      setName = BsonReaderTool.getString(bson, REPL_SET_FIELD, null);
      builder.setSetName(setName);
    } catch (TypesMismatchException ex) {
      throw ex.newWithMessage("Expected \"" + REPL_SET_FIELD
          + "\" field in response to replSetHeartbeat to have type " + "String, but found " + ex
              .getFoundType());
    }

    checkCommandError(bson, setName);

    builder.setHasData(readHasData(bson))
        .setElectionTime(readElectionTime(bson))
        .setTime(readTime(bson))
        .setIsReplSet(BsonReaderTool.isPseudoTrue(bson, IS_REPL_SET_FIELD));
    long term = BsonReaderTool.getLong(bson, TERM_FIELD, -1);

    builder.setTerm(term)
        .setDurableOpTime(readNullableOpTime(bson, DURABLE_OP_TIME_FIELD));

    parseAppliedOpTime(bson, builder, term);

    builder.setIsReplSet(BsonReaderTool.getBoolean(bson, IS_ELECTABLE_FIELD, false))
        .setState(readMemberState(bson))
        .setStateDisagreement(BsonReaderTool.isPseudoTrue(bson, HAS_STATE_DISAGREEMENT_FIELD))
        .setConfigVersion(readConfigVersion(bson, builder.getAppliedOpTime()))
        .setHbmsg(readHbmsg(bson))
        .setSyncingTo(readSyncingTo(bson))
        .setConfig(readConfig(bson));

    return builder.build();
  }

  @Nullable
  private static BsonTimestamp readElectionTime(BsonDocument bson) throws
      TypesMismatchException {
    Entry<?> electionTimeEntry = BsonReaderTool.getEntry(bson,
        ELECTION_TIME_FIELD.getFieldName(), null);
    if (electionTimeEntry != null) {
      switch (electionTimeEntry.getValue().getType()) {
        case DATETIME: {
          return BsonReaderTool.getTimestampFromDateTime(electionTimeEntry);
        }
        case TIMESTAMP:
          return electionTimeEntry.getValue().asTimestamp();
        default: {
          BsonType foundType =
              electionTimeEntry.getValue().getType();
          throw new TypesMismatchException(ELECTION_TIME_FIELD.getFieldName(), "Date or Timestamp",
              foundType, "Expected \"" + ELECTION_TIME_FIELD + "\" field in "
              + "response to replSetHeartbeat command to "
              + "have type Date or Timestamp, but found " + "type " + foundType);
        }
      }
    }
    return null;
  }

  private static boolean readHasData(BsonDocument bson) throws TypesMismatchException,
      NoSuchKeyException {
    if (!bson.containsKey(HAS_DATA_FIELD.getFieldName())) {
      return false;
    }
    return BsonReaderTool.getBoolean(bson, HAS_DATA_FIELD);
  }

  private static void checkCommandError(BsonDocument bson, String setName) throws MongoException {

    if (setName == null && !BsonReaderTool.isPseudoTrue(bson, OK_FIELD)) {
      String errMsg = BsonReaderTool.getString(bson, ERR_MSG_FIELD, "");
      assert errMsg != null;

      Entry<?> errorCodeEntry = BsonReaderTool.getEntry(bson,
          ERROR_CODE_FIELD, null);
      if (errorCodeEntry != null) {
        if (!errorCodeEntry.getValue().isNumber()) {
          throw new BadValueException(ERROR_CODE_FIELD + " is "
              + "not a number");
        }
        ErrorCode errorCode = ErrorCode.fromErrorCode(
            errorCodeEntry.getValue().asNumber().intValue());
        throw new MongoException(errMsg, errorCode);
      }
      throw new UnknownErrorException(errMsg);
    }
  }

  @Nullable
  private static Duration readTime(BsonDocument bson) throws TypesMismatchException {
    BsonNumber timeNumber = BsonReaderTool.getNumeric(bson, TIME_FIELD, null);
    if (timeNumber == null) {
      return null;
    }
    return Duration.ofSeconds(timeNumber.longValue());
  }

  @Nullable
  private static OpTime readNullableOpTime(BsonDocument bson, DocField field) throws
      TypesMismatchException, NoSuchKeyException {
    BsonDocument subDoc = BsonReaderTool.getDocument(bson, field, null);
    if (subDoc == null) {
      return null;
    }
    return OpTime.fromBson(subDoc);
  }

  private static void parseAppliedOpTime(BsonDocument bson, ReplSetHeartbeatReplyBuilder builder,
      long term) throws TypesMismatchException, NoSuchKeyException {
    // In order to support both the 3.0(V0) and 3.2(V1) heartbeats we must parse the OpTime
    // field based on its type. If it is a Date, we parse it as the timestamp and use
    // initialize's term argument to complete the OpTime type. If it is an Object, then it's
    // V1 and we construct an OpTime out of its nested fields.
    Entry<?> entry = BsonReaderTool.getEntry(bson, APPLIED_OP_TIME_FIELD_NAME, null);
    if (entry == null) {
      return;
    }
    BsonValue<?> value = entry.getValue();
    OpTime opTime;
    switch (value.getType()) {
      case TIMESTAMP:
        opTime = new OpTime(value.asTimestamp(), term);
        break;
      case DATETIME:
        BsonTimestamp ts = BsonReaderTool.getTimestampFromDateTime(entry);
        opTime = new OpTime(ts, term);
        break;
      case DOCUMENT: //repl v1
        opTime = OpTime.fromBson(value.asDocument());
        builder.setIsReplSet(true);
        break;
      default:
        throw new TypesMismatchException(APPLIED_OP_TIME_FIELD_NAME, "Date or Timestamp", value
            .getType());
    }
    builder.setAppliedOpTime(opTime);
  }

  @Nullable
  private static MemberState readMemberState(BsonDocument bson) throws BadValueException,
      TypesMismatchException, NoSuchKeyException {
    MemberState state;
    if (!bson.containsKey(MEMBER_STATE_FIELD.getFieldName())) {
      state = null;
    } else {
      int memberId = BsonReaderTool.getNumeric(bson, MEMBER_STATE_FIELD)
          .intValue();
      try {
        state = MemberState.fromId(memberId);
      } catch (IllegalArgumentException ex) {
        throw new BadValueException("Value for \"" + MEMBER_STATE_FIELD + "\" in response to "
            + "replSetHeartbeat is out of range; legal values are "
            + "non-negative and no more than " + MemberState.getMaxId());
      }
    }
    return state;
  }

  private static long readConfigVersion(BsonDocument bson, Optional<OpTime> appliedOpTime) throws
      NoSuchKeyException, TypesMismatchException {
    Entry<?> configVersionEntry = BsonReaderTool.getEntry(bson,
        CONFIG_VERSION_FIELD, null);
    if (appliedOpTime.isPresent() && configVersionEntry == null) {
      throw new NoSuchKeyException(CONFIG_VERSION_FIELD.getFieldName(),
          "Response to replSetHeartbeat missing required \""
          + CONFIG_VERSION_FIELD + "\" field even though "
          + "initialized"
      );
    }
    if (configVersionEntry != null && !configVersionEntry.getValue().isInt32()) {
      throw new TypesMismatchException(CONFIG_VERSION_FIELD.getFieldName(),
          BsonType.INT32, configVersionEntry.getValue().getType(),
          "Expected \"" + CONFIG_VERSION_FIELD + "\" field in "
          + "response to replSetHeartbeat to have type NumberInt, but"
          + " found " + configVersionEntry.getValue().getType()
      );
    }
    if (configVersionEntry != null) {
      return configVersionEntry.getValue().asInt32().getValue();
    }
    return 0;
  }

  @Nonnull
  private static String readHbmsg(BsonDocument bson) throws TypesMismatchException {
    try {
      return BsonReaderTool.getString(bson, HB_MESSAGE_FIELD, "");
    } catch (TypesMismatchException ex) {
      throw ex.newWithMessage("Expected \"" + HB_MESSAGE_FIELD
          + "\" field in response to replSetHeartbeat to have type "
          + "String, but found " + ex.getFoundType());
    }
  }

  @Nullable
  private static HostAndPort readSyncingTo(BsonDocument bson) throws TypesMismatchException {
    try {
      return BsonReaderTool.getHostAndPort(bson, SYNC_SOURCE_FIELD, null);
    } catch (TypesMismatchException ex) {
      throw ex.newWithMessage("Expected \"" + SYNC_SOURCE_FIELD
          + "\" field in response to replSetHeartbeat to have type " + "String, but found " + ex
              .getFoundType());
    }
  }

  @Nullable
  private static ReplicaSetConfig readConfig(BsonDocument bson) throws MongoException,
      FailedToParseException {
    BsonDocument uncastedConf;
    try {
      uncastedConf = BsonReaderTool.getDocument(bson, CONFIG_FIELD, null);
    } catch (TypesMismatchException ex) {
      throw ex.newWithMessage("Expected \"" + CONFIG_FIELD + "\" in response to replSetHeartbeat "
          + "to have type Object, but " + "found " + ex.getFoundType());
    }
    if (uncastedConf == null) {
      return null;
    } else {
      return ReplicaSetConfig.fromDocument(uncastedConf);
    }
  }

}
