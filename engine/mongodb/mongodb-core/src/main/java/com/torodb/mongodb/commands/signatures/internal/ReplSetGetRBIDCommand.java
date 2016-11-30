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
import com.eightkdata.mongowp.fields.IntField;
import com.eightkdata.mongowp.server.api.impl.AbstractNotAliasableCommand;
import com.eightkdata.mongowp.server.api.tools.Empty;
import com.eightkdata.mongowp.utils.BsonDocumentBuilder;
import com.torodb.mongodb.commands.signatures.internal.ReplSetGetRBIDCommand.ReplSetGetRBIDReply;
import com.torodb.mongodb.commands.tools.EmptyCommandArgumentMarshaller;

@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public class ReplSetGetRBIDCommand
    extends AbstractNotAliasableCommand<Empty, ReplSetGetRBIDReply> {

  public static final ReplSetGetRBIDCommand INSTANCE = new ReplSetGetRBIDCommand();

  private ReplSetGetRBIDCommand() {
    super("replSetGetRBID");
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
  public Class<? extends ReplSetGetRBIDReply> getResultClass() {
    return ReplSetGetRBIDReply.class;
  }

  @Override
  public BsonDocument marshallResult(ReplSetGetRBIDReply reply) {
    return reply.marshall();
  }

  @Override
  public ReplSetGetRBIDReply unmarshallResult(BsonDocument resultDoc) {
    throw new UnsupportedOperationException("Not supported yet."); //TODO
  }

  public static class ReplSetGetRBIDReply {

    private static final IntField RBID_FIELD = new IntField("rbid");

    private final int rbid;

    public ReplSetGetRBIDReply(int rbid) {
      this.rbid = rbid;
    }

    public int getRBID() {
      return rbid;
    }

    private BsonDocument marshall() {
      BsonDocumentBuilder builder = new BsonDocumentBuilder();

      builder.append(RBID_FIELD, rbid);
      return builder.build();
    }
  }

}
