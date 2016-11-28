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

package com.torodb.mongodb.commands.signatures.repl;

import com.eightkdata.mongowp.ErrorCode;
import com.eightkdata.mongowp.MongoConstants;
import com.eightkdata.mongowp.OpTime;
import com.eightkdata.mongowp.bson.BsonDocument;
import com.eightkdata.mongowp.bson.BsonValue;
import com.eightkdata.mongowp.bson.utils.DefaultBsonValues;
import com.eightkdata.mongowp.exceptions.MongoException;
import com.eightkdata.mongowp.fields.ArrayField;
import com.eightkdata.mongowp.fields.BooleanField;
import com.eightkdata.mongowp.fields.DateTimeField;
import com.eightkdata.mongowp.fields.DoubleField;
import com.eightkdata.mongowp.fields.HostAndPortField;
import com.eightkdata.mongowp.fields.IntField;
import com.eightkdata.mongowp.fields.StringField;
import com.eightkdata.mongowp.fields.TimestampField;
import com.eightkdata.mongowp.server.api.impl.AbstractNotAliasableCommand;
import com.eightkdata.mongowp.server.api.tools.Empty;
import com.eightkdata.mongowp.utils.BsonDocumentBuilder;
import com.google.common.collect.Lists;
import com.google.common.net.HostAndPort;
import com.google.common.primitives.UnsignedInteger;
import com.torodb.mongodb.commands.pojos.MemberConfig;
import com.torodb.mongodb.commands.pojos.MemberHeartbeatData;
import com.torodb.mongodb.commands.pojos.MemberState;
import com.torodb.mongodb.commands.pojos.ReplicaSetConfig;
import com.torodb.mongodb.commands.signatures.repl.ReplSetGetStatusCommand.ReplSetGetStatusReply;
import com.torodb.mongodb.commands.tools.EmptyCommandArgumentMarshaller;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

/**
 * Report status of a replica set from the POV of this server.
 */
@Immutable
public class ReplSetGetStatusCommand
    extends AbstractNotAliasableCommand<Empty, ReplSetGetStatusReply> {

  public static final ReplSetGetStatusCommand INSTANCE = new ReplSetGetStatusCommand();

  private ReplSetGetStatusCommand() {
    super("replSetGetStatus");
  }

  @Override
  public Class<? extends Empty> getArgClass() {
    return Empty.class;
  }

  @Override
  public Empty unmarshallArg(BsonDocument requestDoc) {
    return Empty.getInstance();
  }

  @Override
  public BsonDocument marshallArg(Empty request) {
    return EmptyCommandArgumentMarshaller.marshallEmptyArgument(this);
  }

  @Override
  public Class<? extends ReplSetGetStatusReply> getResultClass() {
    return ReplSetGetStatusReply.class;
  }

  @Override
  public BsonDocument marshallResult(ReplSetGetStatusReply reply) {
    return reply.marshall();
  }

  @Override
  public ReplSetGetStatusReply unmarshallResult(BsonDocument resultDoc) throws
      MongoException, UnsupportedOperationException {
    throw new UnsupportedOperationException("Not supported");
  }

  @Immutable
  public abstract static class ReplSetGetStatusReply {

    public abstract ErrorCode getErrorCode();

    public abstract String getErrMsg();

    public abstract String getSetName();

    public abstract MemberState getMyState();

    public abstract Instant getDate();

    protected abstract BsonDocument marshall();
  }

  @Immutable
  public static class InvalidReplSetGetStatusReply extends ReplSetGetStatusReply {
    @Nonnull private final ErrorCode errorCode;
    @Nonnull private final String errMsg;
    @Nonnull private final MemberState state;
    @Nonnull private final Duration uptime;
    @Nonnull private final OpTime optime;
    @Nonnegative private final int maintenanceModeCalls;
    @Nullable private final String heartbeatMessage;

    public InvalidReplSetGetStatusReply(
        @Nonnull ErrorCode errorCode,
        @Nonnull String errMsg,
        MemberState state,
        Duration uptime,
        OpTime optime,
        int maintenanceModeCalls,
        String heartbeatMessage) {
      this.errorCode = errorCode;
      this.errMsg = errMsg;
      this.state = state;
      this.uptime = uptime;
      this.optime = optime;
      this.maintenanceModeCalls = maintenanceModeCalls;
      this.heartbeatMessage = heartbeatMessage;
    }

    @Override
    public ErrorCode getErrorCode() {
      return errorCode;
    }

    @Override
    public String getErrMsg() {
      return errMsg;
    }

    @Override
    public String getSetName() {
      return null;
    }

    @Override
    public MemberState getMyState() {
      return state;
    }

    @Override
    public Instant getDate() {
      return null;
    }

    private static final StringField ERR_MSG_FIELD_NAME = new StringField("errmsg");
    private static final IntField ERROR_CODE_FIELD_NAME = new IntField("code");
    private static final DoubleField OK_FIELD_NAME = new DoubleField("ok");
    private static final IntField STATE_FIELD = new IntField("state");
    private static final StringField STATE_STR_FIELD = new StringField("stateStr");
    private static final IntField UPTIME_FIELD = new IntField("uptime");
    private static final TimestampField OPTIME_FIELD = new TimestampField("optime");
    private static final DateTimeField OPTIME_DATE_FIELD = new DateTimeField("optimeDate");
    private static final IntField MAINTENANCE_MODE_FIELD = new IntField("maintenanceMode");
    private static final StringField INFO_MESSAGE_FIELD = new StringField("infoMessage");

    @Override
    protected BsonDocument marshall() {
      BsonDocumentBuilder builder = new BsonDocumentBuilder();
      builder.append(ERR_MSG_FIELD_NAME, errMsg)
          .append(ERROR_CODE_FIELD_NAME, ErrorCode.INVALID_REPLICA_SET_CONFIG.getErrorCode())
          .append(OK_FIELD_NAME, MongoConstants.KO)
          .append(STATE_FIELD, state.getId())
          .append(STATE_STR_FIELD, state.name())
          //TODO: Check if cast to int is correct
          .append(UPTIME_FIELD, UnsignedInteger.valueOf(uptime.getSeconds()).intValue())
          .append(OPTIME_FIELD, optime)
          .appendInstant(OPTIME_DATE_FIELD, DefaultBsonValues.newDateTime(optime.getTimestamp())
              .getMillisFromUnix());
      if (maintenanceModeCalls != 0) {
        builder.append(MAINTENANCE_MODE_FIELD, maintenanceModeCalls);
      }
      if (heartbeatMessage != null) {
        builder.append(INFO_MESSAGE_FIELD, heartbeatMessage);
      }

      return builder.build();
    }
  }

  public static class CorrectReplSetGetStatusReply extends ReplSetGetStatusReply {

    private static final StringField SET_NAME_FIELD = new StringField("set");
    private static final DateTimeField DATE_FIELD = new DateTimeField("date");
    private static final IntField MY_STATE_FIELD = new IntField("myState");
    private static final HostAndPortField SYNCING_TO_FIELD = new HostAndPortField("syncingTo");
    private static final ArrayField MEMBERS_FIELD = new ArrayField("members");

    private static final DoubleField OK_FIELD_NAME = new DoubleField("ok");
    private static final IntField MEMBER_ID_FIELD = new IntField("_id");
    private static final HostAndPortField MEMBER_NAME_FIELD = new HostAndPortField("name");
    private static final DoubleField MEMBER_HEALTH_FIELD = new DoubleField("health");
    private static final IntField MEMBER_STATE_FIELD = new IntField("state");
    private static final StringField MEMBER_STATE_STR_FIELD = new StringField("stateStr");
    private static final IntField MEMBER_UPTIME_FIELD = new IntField("uptime");
    private static final TimestampField MEMBER_OPTIME_FIELD = new TimestampField("optime");
    private static final DateTimeField MEMBER_OPTIME_DATE_FIELD = new DateTimeField("optimeDate");
    private static final HostAndPortField MEMBER_SYNCING_TO_FIELD =
        new HostAndPortField("syncingTo");
    private static final IntField MEMBER_MAINTENANCE_MODE_FIELD = new IntField("maintenanceMode");
    private static final StringField MEMBER_INFO_MESSAGE_FIELD = new StringField("infoMessage");
    private static final TimestampField MEMBER_ELECTION_TIME_FIELD = new TimestampField(
        "electionTime");
    private static final DateTimeField MEMBER_ELECTION_DATE_FIELD =
        new DateTimeField("electionDate");
    private static final DoubleField MEMBER_CONFIG_VERSION_FIELD = new DoubleField("configVersion");
    private static final BooleanField MEMBER_SELF_FIELD = new BooleanField("self");
    private static final DateTimeField MEMBER_LAST_HEARTBEAT = new DateTimeField("lastHeartbeat");
    private static final DateTimeField MEMBER_LAST_HEARTBEAT_RECIVED = new DateTimeField(
        "lastHeartbeatRecv");
    private static final IntField MEMBER_PING_MS = new IntField("pingMs");
    private static final StringField MEMBER_LAST_HEARTBEAT_MESSAGE = new StringField(
        "lastHeartbeatMessage");
    private static final BooleanField MEMBER_AUTHENTICATED = new BooleanField("authenticated");

    @Nonnull
    private final SelfData selfData;
    @Nonnull
    private final String setName;
    @Nonnull
    private final Instant now;
    @Nullable
    private final HostAndPort syncTo;
    private final Map<MemberConfig, MemberHeartbeatData> membersInfo;
    private final Map<MemberConfig, Integer> pings;
    @Nonnull
    private final ReplicaSetConfig replConfig;

    public CorrectReplSetGetStatusReply(
        @Nonnull SelfData selfData,
        @Nonnull String setName,
        @Nonnull Instant now,
        @Nullable HostAndPort syncTo,
        Map<MemberConfig, MemberHeartbeatData> membersInfo,
        Map<MemberConfig, Integer> pings,
        @Nonnull ReplicaSetConfig replConfig) {
      this.selfData = selfData;
      this.setName = setName;
      this.now = now;
      this.syncTo = syncTo;
      this.membersInfo = membersInfo;
      this.pings = pings;
      this.replConfig = replConfig;
    }

    @Override
    public ErrorCode getErrorCode() {
      return ErrorCode.OK;
    }

    @Override
    public String getErrMsg() {
      return "";
    }

    @Override
    public String getSetName() {
      return setName;
    }

    @Override
    public MemberState getMyState() {
      return null;
    }

    @Override
    public Instant getDate() {
      return now;
    }

    @Override
    protected BsonDocument marshall() {
      BsonDocumentBuilder builder = new BsonDocumentBuilder();

      MemberState state = selfData.state;

      builder.append(SET_NAME_FIELD, setName)
          .append(DATE_FIELD, now)
          .append(MY_STATE_FIELD, state.getId());

      if (syncTo != null && !state.equals(MemberState.RS_PRIMARY) && !state.equals(
          MemberState.RS_REMOVED)) {
        builder.append(SYNCING_TO_FIELD, syncTo);
      }

      List<BsonValue<?>> membersList = Lists.newArrayListWithCapacity(membersInfo.size());
      for (Entry<MemberConfig, MemberHeartbeatData> entrySet : membersInfo.entrySet()) {
        MemberConfig memberConfig = entrySet.getKey();
        MemberHeartbeatData memberData = entrySet.getValue();
        if (memberConfig.getId() == selfData.getId()) {
          membersList.add(marshallSelfMember());
        } else {
          membersList.add(marshallOtherMember(memberConfig, memberData));
        }
      }
      //TODO: find out how mongo actually sort the members array
      Collections.sort(membersList, (BsonValue<?> o1, BsonValue<?> o2) -> {
        BsonValue<?> v1 = o1.asDocument().get(MEMBER_ID_FIELD.getFieldName());
        assert v1 != null;
        BsonValue<?> v2 = o2.asDocument().get(MEMBER_ID_FIELD.getFieldName());
        return v1.compareTo(v2);
      });

      builder.append(MEMBERS_FIELD, DefaultBsonValues.newArray(membersList));

      return builder.build();
    }

    private BsonDocument marshallSelfMember() {
      BsonDocumentBuilder builder = new BsonDocumentBuilder();

      builder.append(MEMBER_ID_FIELD, selfData.getId())
          .append(MEMBER_NAME_FIELD, selfData.getName())
          .append(MEMBER_HEALTH_FIELD, 1.0)
          .append(OK_FIELD_NAME, MongoConstants.OK)
          .append(MEMBER_STATE_FIELD, selfData.getState().getId())
          .append(MEMBER_STATE_STR_FIELD, selfData.getState().name())
          .append(MEMBER_UPTIME_FIELD, UnsignedInteger.valueOf(selfData.getUptime().getSeconds())
              .intValue()); //TODO: Check if cast to int is correct

      if (!selfData.getState().equals(MemberState.RS_ARBITER)) {
        builder.append(MEMBER_OPTIME_FIELD, selfData.getOpTime());
        selfData.getOpTime().appendAsOldBson(builder, MEMBER_OPTIME_DATE_FIELD);
      }
      if (syncTo != null && !selfData.getState().equals(MemberState.RS_PRIMARY)) {
        builder.append(MEMBER_SYNCING_TO_FIELD, syncTo);
      }
      if (selfData.getMaintenanceModeCalls() != 0) {
        builder.append(MEMBER_MAINTENANCE_MODE_FIELD, selfData.getMaintenanceModeCalls());
      }
      if (selfData.getHeartbeatMessage() != null && !selfData.getHeartbeatMessage().isEmpty()) {
        builder.append(MEMBER_INFO_MESSAGE_FIELD, selfData.getHeartbeatMessage());
      }
      if (selfData.getState().equals(MemberState.RS_PRIMARY)) {
        builder.append(MEMBER_ELECTION_TIME_FIELD, selfData.getElectionTime());
        selfData.getElectionTime().appendAsOldBson(builder, MEMBER_ELECTION_DATE_FIELD);
      }
      builder.appendNumber(MEMBER_CONFIG_VERSION_FIELD, replConfig.getConfigVersion());
      builder.append(MEMBER_SELF_FIELD, true);

      return builder.build();
    }

    private BsonDocument marshallOtherMember(MemberConfig config, MemberHeartbeatData data) {
      BsonDocumentBuilder builder = new BsonDocumentBuilder();

      builder.append(MEMBER_ID_FIELD, config.getId())
          .append(MEMBER_NAME_FIELD, config.getHostAndPort());
      MemberHeartbeatData.Health h = data.getHealth();
      builder.append(MEMBER_HEALTH_FIELD, h.getId())
          .append(MEMBER_STATE_FIELD, data.getState().getId());

      if (h.equals(MemberHeartbeatData.Health.UNREACHABLE)) {
        builder.append(MEMBER_STATE_STR_FIELD, "(not reachable/healthy)");
      } else {
        builder.append(MEMBER_STATE_STR_FIELD, data.getState().name());
      }
      Instant upSince = data.getUpSince();
      if (upSince == null || upSince.equals(Instant.EPOCH)) {
        builder.append(MEMBER_UPTIME_FIELD, 0);
      } else {
        int uptime = (int) (now.getEpochSecond() - upSince.getEpochSecond());
        builder.append(MEMBER_UPTIME_FIELD, uptime);
      }
      if (!config.isArbiter()) {
        OpTime opTime = data.getOpTime();
        builder.append(MEMBER_OPTIME_FIELD, opTime);
        builder.appendUnsafe(MEMBER_OPTIME_DATE_FIELD.getFieldName(), opTime.toOldBson());
      }
      builder.append(MEMBER_LAST_HEARTBEAT, data.getLastHeartbeat());
      builder.append(MEMBER_LAST_HEARTBEAT_RECIVED, data.getLastHeartbeatRecv());
      Integer ping = pings.get(config);
      if (ping != null) {
        builder.append(MEMBER_PING_MS, ping);
        String hbmsg = data.getLastHeartbeatMessage();
        builder.append(MEMBER_LAST_HEARTBEAT_MESSAGE, hbmsg);
      }
      if (data.isAuthIssue()) {
        builder.append(MEMBER_AUTHENTICATED, true);
      }
      if (!data.getState().equals(MemberState.RS_PRIMARY)) {
        if (data.getSyncSource() != null) {
          builder.append(MEMBER_SYNCING_TO_FIELD, data.getSyncSource());
        }
      } else {
        //is primary
        builder.append(MEMBER_ELECTION_TIME_FIELD, data.getElectionTime());
        //TODO(gortiz): Check if that is consistent with MongoDB source code
        builder.appendInstant(MEMBER_ELECTION_DATE_FIELD, data.getElectionTime()
            .getSecondsSinceEpoch());
      }

      builder.appendNumber(MEMBER_CONFIG_VERSION_FIELD, data.getConfigVersion());

      return builder.build();
    }

    public static final class SelfData {

      private final int id;
      private final HostAndPort name;
      private final MemberState state;
      @Nonnull
      private final Duration uptime;
      @Nonnull
      private final OpTime opTime;
      private final int maintenanceModeCalls;
      @Nullable
      private final String heartbeatMessage;
      private final OpTime electionTime;

      public SelfData(int id, HostAndPort name, MemberState state, Duration uptime, OpTime opTime,
          int maintenanceModeCalls, String heartbeatMessage, OpTime electionTime) {
        this.id = id;
        this.name = name;
        this.state = state;
        this.uptime = uptime;
        this.opTime = opTime;
        this.maintenanceModeCalls = maintenanceModeCalls;
        this.heartbeatMessage = heartbeatMessage;
        this.electionTime = electionTime;
      }

      public int getId() {
        return id;
      }

      public HostAndPort getName() {
        return name;
      }

      public MemberState getState() {
        return state;
      }

      public Duration getUptime() {
        return uptime;
      }

      public OpTime getOpTime() {
        return opTime;
      }

      public int getMaintenanceModeCalls() {
        return maintenanceModeCalls;
      }

      public String getHeartbeatMessage() {
        return heartbeatMessage;
      }

      public OpTime getElectionTime() {
        return electionTime;
      }
    }
  }
}
