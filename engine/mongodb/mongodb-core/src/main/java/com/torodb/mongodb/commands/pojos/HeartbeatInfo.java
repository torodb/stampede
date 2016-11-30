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

package com.torodb.mongodb.commands.pojos;

import com.eightkdata.mongowp.bson.BsonDocument;
import com.eightkdata.mongowp.exceptions.BadValueException;
import com.eightkdata.mongowp.exceptions.NoSuchKeyException;
import com.eightkdata.mongowp.exceptions.TypesMismatchException;
import com.eightkdata.mongowp.fields.BooleanField;
import com.eightkdata.mongowp.fields.LongField;
import com.eightkdata.mongowp.fields.StringField;
import com.eightkdata.mongowp.utils.BsonDocumentBuilder;
import com.eightkdata.mongowp.utils.BsonReaderTool;
import com.google.common.net.HostAndPort;

import javax.annotation.Nullable;

public class HeartbeatInfo {

  private static final BooleanField CHECK_EMPTY_FIELD_NAME = new BooleanField("checkEmpty");
  private static final LongField PROTOCOL_VERSION_FIELD_NAME = new LongField("pv");
  private static final LongField CONFIG_VERSION_FIELD_NAME = new LongField("v");
  private static final LongField SENDER_ID_FIELD_NAME = new LongField("fromId");
  private static final StringField SET_NAME_FIELD_NAME = new StringField("replSetHeartbeat");
  private static final StringField SENDER_HOST_FIELD_NAME = new StringField("from");

  private final Boolean checkEmpty;
  private final long protocolVersion;
  private final long configVersion;
  private final Long senderId;
  private final String setName;
  private final HostAndPort senderHost;

  public HeartbeatInfo(
      long protocolVersion,
      long configVersion,
      String setName,
      HostAndPort senderHost,
      @Nullable Long senderId,
      @Nullable Boolean checkEmpty) {
    this.checkEmpty = checkEmpty;
    this.protocolVersion = protocolVersion;
    this.configVersion = configVersion;
    this.senderId = senderId;
    this.setName = setName;
    this.senderHost = senderHost;
  }

  public boolean isCheckEmpty() {
    return checkEmpty;
  }

  public long getProtocolVersion() {
    return protocolVersion;
  }

  public long getConfigVersion() {
    return configVersion;
  }

  @Nullable
  public Long getSenderId() {
    return senderId;
  }

  public String getSetName() {
    return setName;
  }

  public HostAndPort getSenderHost() {
    return senderHost;
  }

  public static HeartbeatInfo unmarshall(BsonDocument bson) throws TypesMismatchException,
      NoSuchKeyException, BadValueException {
    BsonReaderTool.checkOnlyHasFields(
        "ReplSetHeartbeatArgs",
        bson,
        CHECK_EMPTY_FIELD_NAME.getFieldName(),
        PROTOCOL_VERSION_FIELD_NAME.getFieldName(),
        CONFIG_VERSION_FIELD_NAME.getFieldName(),
        SENDER_ID_FIELD_NAME.getFieldName(),
        SET_NAME_FIELD_NAME.getFieldName(),
        SENDER_HOST_FIELD_NAME.getFieldName()
    );
    Boolean checkEmpty = null;
    if (bson.containsKey(CHECK_EMPTY_FIELD_NAME.getFieldName())) {
      checkEmpty = BsonReaderTool.getBoolean(bson, CHECK_EMPTY_FIELD_NAME);
    }
    long protocolVersion = BsonReaderTool.getLong(bson, PROTOCOL_VERSION_FIELD_NAME);
    long configVersion = BsonReaderTool.getLong(bson, CONFIG_VERSION_FIELD_NAME);
    Long senderId = null;
    if (bson.containsKey(SENDER_ID_FIELD_NAME.getFieldName())) {
      senderId = BsonReaderTool.getLong(bson, SENDER_ID_FIELD_NAME);
    }
    String setName = BsonReaderTool.getString(bson, SET_NAME_FIELD_NAME);

    String senderHostString = BsonReaderTool.getString(bson, SENDER_HOST_FIELD_NAME, null);
    HostAndPort senderHost = senderHostString != null ? BsonReaderTool.getHostAndPort(
        senderHostString) : null;

    return new HeartbeatInfo(protocolVersion, configVersion, setName, senderHost, senderId,
        checkEmpty);
  }

  public BsonDocument marshall() {
    BsonDocumentBuilder builder = new BsonDocumentBuilder();
    builder.append(SET_NAME_FIELD_NAME, setName);
    builder.append(PROTOCOL_VERSION_FIELD_NAME, protocolVersion);
    builder.append(CONFIG_VERSION_FIELD_NAME, configVersion);
    if (senderHost != null) {
      builder.append(SENDER_HOST_FIELD_NAME, senderHost.toString());
    } else {
      builder.append(SENDER_HOST_FIELD_NAME, "");
    }

    if (senderId != null) {
      builder.append(SENDER_ID_FIELD_NAME, senderId);
    }
    if (checkEmpty != null) {
      builder.append(CHECK_EMPTY_FIELD_NAME, checkEmpty);
    }
    return builder.build();
  }

}
