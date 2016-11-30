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

import com.eightkdata.mongowp.bson.BsonDocument;
import com.eightkdata.mongowp.bson.BsonObjectId;
import com.eightkdata.mongowp.exceptions.BadValueException;
import com.eightkdata.mongowp.exceptions.NoSuchKeyException;
import com.eightkdata.mongowp.exceptions.TypesMismatchException;
import com.eightkdata.mongowp.fields.DocField;
import com.eightkdata.mongowp.fields.IntField;
import com.eightkdata.mongowp.fields.ObjectIdField;
import com.eightkdata.mongowp.server.api.impl.AbstractNotAliasableCommand;
import com.eightkdata.mongowp.server.api.tools.Empty;
import com.eightkdata.mongowp.utils.BsonDocumentBuilder;
import com.eightkdata.mongowp.utils.BsonReaderTool;
import com.google.common.collect.ImmutableSet;
import com.torodb.mongodb.commands.pojos.MemberConfig;
import com.torodb.mongodb.commands.signatures.internal.HandshakeCommand.HandshakeArgument;

import javax.annotation.Nullable;

public class HandshakeCommand extends AbstractNotAliasableCommand<HandshakeArgument, Empty> {

  public static final HandshakeCommand INSTANCE = new HandshakeCommand();

  private static final String RID_FIELD_NAME = "handshake";

  protected HandshakeCommand() {
    super(RID_FIELD_NAME);
  }

  @Override
  public Class<? extends HandshakeArgument> getArgClass() {
    return HandshakeArgument.class;
  }

  @Override
  public HandshakeArgument unmarshallArg(BsonDocument requestDoc)
      throws BadValueException, TypesMismatchException, NoSuchKeyException {
    return HandshakeArgument.unmarshall(requestDoc);
  }

  @Override
  public BsonDocument marshallArg(HandshakeArgument request) {
    return request.marshall();
  }

  @Override
  public Class<? extends Empty> getResultClass() {
    return Empty.class;
  }

  @Override
  public BsonDocument marshallResult(Empty reply) {
    return null;
  }

  @Override
  public Empty unmarshallResult(BsonDocument resultDoc) {
    return Empty.getInstance();
  }

  public static class HandshakeArgument {

    private static final IntField REPL_SET_UPDATE_POSITION_FIELD = new IntField(
        "replSetUpdatePosition");
    private static final DocField HANDSHAKE_OBJ_FIELD = new DocField("handshake");
    private static final ObjectIdField RID_FIELD = new ObjectIdField("handshake");
    private static final IntField MEMBER_FIELD = new IntField("member");
    private static final DocField CONFIG_FIELD = new DocField("config");

    private static final ImmutableSet<String> VALID_FIELD_NAMES = ImmutableSet.of(
        REPL_SET_UPDATE_POSITION_FIELD.getFieldName(), HANDSHAKE_OBJ_FIELD.getFieldName(),
        RID_FIELD.getFieldName(), MEMBER_FIELD.getFieldName(), CONFIG_FIELD.getFieldName()
    );

    private final BsonObjectId rid;
    private final Integer memberId;
    /**
     * This is not used on MongoDB 3.0.0 and higher, but it is required in older versions.
     */
    @Nullable
    private final MemberConfig config;

    public HandshakeArgument(
        BsonObjectId rid,
        @Nullable Integer memberId,
        @Nullable MemberConfig config) {
      this.rid = rid;
      this.memberId = memberId;
      this.config = config;
    }

    public BsonObjectId getRid() {
      return rid;
    }

    public Integer getMemberId() {
      return memberId;
    }

    public MemberConfig getConfig() {
      return config;
    }

    @Override
    public String toString() {
      return HandshakeCommand.INSTANCE.marshallArg(this).toString();
    }

    private static HandshakeArgument unmarshall(BsonDocument requestDoc)
        throws TypesMismatchException, NoSuchKeyException, BadValueException {
      //TODO: CHECK UNMARSHALLING;
      BsonReaderTool.checkOnlyHasFields("HandshakeArgs", requestDoc, VALID_FIELD_NAMES);

      BsonObjectId rid = BsonReaderTool.getObjectId(requestDoc, RID_FIELD);
      Integer memberId;
      if (!requestDoc.containsKey(MEMBER_FIELD.getFieldName())) {
        memberId = null;
      } else {
        memberId = BsonReaderTool.getInteger(requestDoc, MEMBER_FIELD);
      }
      BsonDocument configBson = BsonReaderTool.getDocument(
          requestDoc,
          CONFIG_FIELD,
          null
      );
      MemberConfig memberConfig = null;
      if (configBson != null) {
        memberConfig = MemberConfig.fromDocument(configBson);
      }

      return new HandshakeArgument(rid, memberId, memberConfig);
    }

    private BsonDocument marshall() {
      return new BsonDocumentBuilder()
          .append(RID_FIELD, rid)
          .append(MEMBER_FIELD, memberId)
          .append(CONFIG_FIELD, config.toBson())
          .build();
    }

    public BsonDocument marshallAsReplSetUpdate() {
      BsonDocumentBuilder builder = new BsonDocumentBuilder();
      builder.append(REPL_SET_UPDATE_POSITION_FIELD, 1);
      builder.append(HANDSHAKE_OBJ_FIELD, marshall());
      return builder.build();
    }
  }

}
