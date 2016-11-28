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

import com.eightkdata.mongowp.OpTime;
import com.eightkdata.mongowp.bson.BsonDateTime;
import com.eightkdata.mongowp.bson.BsonDocument;
import com.eightkdata.mongowp.bson.BsonTimestamp;
import com.eightkdata.mongowp.bson.utils.DefaultBsonValues;
import com.eightkdata.mongowp.bson.utils.TimestampToDateTime;
import com.eightkdata.mongowp.exceptions.BadValueException;
import com.eightkdata.mongowp.exceptions.NoSuchKeyException;
import com.eightkdata.mongowp.exceptions.TypesMismatchException;
import com.eightkdata.mongowp.fields.BooleanField;
import com.eightkdata.mongowp.fields.DateTimeField;
import com.eightkdata.mongowp.fields.HostAndPortField;
import com.eightkdata.mongowp.fields.IntField;
import com.eightkdata.mongowp.fields.LongField;
import com.eightkdata.mongowp.fields.StringField;
import com.eightkdata.mongowp.server.api.impl.AbstractNotAliasableCommand;
import com.eightkdata.mongowp.utils.BsonDocumentBuilder;
import com.eightkdata.mongowp.utils.BsonReaderTool;
import com.google.common.net.HostAndPort;
import com.torodb.mongodb.commands.signatures.internal.ReplSetFreshCommand.ReplSetFreshArgument;
import com.torodb.mongodb.commands.signatures.internal.ReplSetFreshCommand.ReplSetFreshReply;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 *
 */
public class ReplSetFreshCommand
    extends AbstractNotAliasableCommand<ReplSetFreshArgument, ReplSetFreshReply> {

  private static final IntField COMMAND_FIELD = new IntField("replSetFresh");

  public static final ReplSetFreshCommand INSTANCE = new ReplSetFreshCommand();

  private ReplSetFreshCommand() {
    super(COMMAND_FIELD.getFieldName());
  }

  @Override
  public Class<? extends ReplSetFreshArgument> getArgClass() {
    return ReplSetFreshArgument.class;
  }

  @Override
  public ReplSetFreshArgument unmarshallArg(BsonDocument requestDoc)
      throws TypesMismatchException, NoSuchKeyException, BadValueException {
    return ReplSetFreshArgument.unmarshall(requestDoc);
  }

  @Override
  public BsonDocument marshallArg(ReplSetFreshArgument request) {
    return request.marshall();
  }

  @Override
  public Class<? extends ReplSetFreshReply> getResultClass() {
    return ReplSetFreshReply.class;
  }

  @Override
  public BsonDocument marshallResult(ReplSetFreshReply reply) {
    return reply.marshall();
  }

  @Override
  public ReplSetFreshReply unmarshallResult(BsonDocument resultDoc) {
    throw new UnsupportedOperationException("Not implemented yet!"); //TODO
  }

  public static class ReplSetFreshArgument {

    private static final StringField SET_NAME_FIELD = new StringField("set");
    private static final HostAndPortField WHO_FIELD = new HostAndPortField("who");
    private static final IntField ID_FIELD = new IntField("id");
    private static final LongField CFG_VER_FIELD = new LongField("cfgver");
    private static final DateTimeField OPTIME_FIELD = new DateTimeField("opTime");

    private final String setName;
    private final HostAndPort who;
    private final int clientId;
    private final long cfgVersion;
    private final BsonTimestamp opTime;

    public ReplSetFreshArgument(
        String setName,
        HostAndPort who,
        int clientId,
        long cfgVersion,
        BsonTimestamp opTime) {
      this.setName = setName;
      this.who = who;
      this.clientId = clientId;
      this.cfgVersion = cfgVersion;
      this.opTime = opTime;
    }

    /**
     *
     * @return the name of the set
     */
    public String getSetName() {
      return setName;
    }

    /**
     *
     * @return the host and port of the member that sent the request
     */
    public HostAndPort getWho() {
      return who;
    }

    /**
     *
     * @return the repl set of the member that sent the request
     */
    public int getClientId() {
      return clientId;
    }

    /**
     *
     * @return replSet config version that the member who sent the request
     */
    public long getCfgVersion() {
      return cfgVersion;
    }

    /**
     *
     * @return last optime seen by the member who sent the request
     */
    public BsonTimestamp getOpTime() {
      return opTime;
    }

    private BsonDocument marshall() {
      BsonDocumentBuilder builder = new BsonDocumentBuilder();

      builder.append(COMMAND_FIELD, 1);
      builder.append(SET_NAME_FIELD, setName);
      BsonDateTime dateTimeOpTime = TimestampToDateTime.toDateTime(
          opTime, DefaultBsonValues::newDateTime);
      builder.append(OPTIME_FIELD, dateTimeOpTime.getValue());
      builder.append(WHO_FIELD, who);
      builder.append(CFG_VER_FIELD, cfgVersion);
      builder.append(ID_FIELD, clientId);

      return builder.build();
    }

    private static ReplSetFreshArgument unmarshall(BsonDocument bson) throws
        TypesMismatchException, NoSuchKeyException {
      int clientId = BsonReaderTool.getInteger(bson, ID_FIELD);
      String setName = BsonReaderTool.getString(bson, SET_NAME_FIELD);
      HostAndPort who = BsonReaderTool.getHostAndPort(bson, WHO_FIELD);
      long cfgver = BsonReaderTool.getNumeric(bson, CFG_VER_FIELD).longValue();
      BsonTimestamp optime = BsonReaderTool.getTimestampFromDateTime(bson, OPTIME_FIELD);

      return new ReplSetFreshArgument(setName, who, clientId, cfgver, optime);
    }

  }

  public static class ReplSetFreshReply {

    private static final BooleanField FRESHER_FIELD = new BooleanField("fresher");
    private static final StringField INFO_FIELD = new StringField("info");
    private static final StringField ERRMSG_FIELD = new StringField("errmsg");
    //Note: MongoDB usually stores and send optimes as datetimes
    private static final DateTimeField OPTIME_FIELD = new DateTimeField("optime");
    private static final BooleanField VETO_FIELD = new BooleanField("veto");

    private final String info;
    private final String vetoMessage;
    private final OpTime opTime;
    private final boolean weAreFresher;
    private final boolean doVeto;

    /**
     *
     * @param info
     * @param vetoMessage  the reason why the votation is vetoed. If null, then this node is not
     *                     vetoing.
     * @param opTime
     * @param weAreFresher
     */
    public ReplSetFreshReply(
        @Nullable String info,
        @Nullable String vetoMessage,
        @Nonnull OpTime opTime,
        boolean weAreFresher) {
      this.info = info;
      this.vetoMessage = vetoMessage;
      this.opTime = opTime;
      this.weAreFresher = weAreFresher;
      this.doVeto = vetoMessage != null;
    }

    @Nullable
    public String getInfo() {
      return info;
    }

    @Nullable
    public String getVetoMessage() {
      return vetoMessage;
    }

    @Nonnull
    public OpTime getOpTime() {
      return opTime;
    }

    public boolean isWeAreFresher() {
      return weAreFresher;
    }

    public boolean isDoVeto() {
      return doVeto;
    }

    private BsonDocument marshall() {
      BsonDocumentBuilder result = new BsonDocumentBuilder();

      result.append(FRESHER_FIELD, weAreFresher);
      if (getInfo() != null) {
        result.append(INFO_FIELD, info);
      }
      if (getVetoMessage() != null) {
        result.append(ERRMSG_FIELD, vetoMessage);
      }
      result.append(OPTIME_FIELD, DefaultBsonValues.newDateTime(
          opTime.getTimestamp()).getValue());
      result.append(VETO_FIELD, isDoVeto());

      return result.build();
    }
  }

}
