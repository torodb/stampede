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

import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.CommandImplementation;
import com.eightkdata.mongowp.server.api.tools.Empty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.torodb.mongodb.commands.signatures.internal.HandshakeCommand.HandshakeArgument;
import com.torodb.mongodb.commands.signatures.internal.ReplSetElectCommand.ReplSetElectArgument;
import com.torodb.mongodb.commands.signatures.internal.ReplSetElectCommand.ReplSetElectReply;
import com.torodb.mongodb.commands.signatures.internal.ReplSetFreshCommand.ReplSetFreshArgument;
import com.torodb.mongodb.commands.signatures.internal.ReplSetFreshCommand.ReplSetFreshReply;
import com.torodb.mongodb.commands.signatures.internal.ReplSetGetRBIDCommand.ReplSetGetRBIDReply;
import com.torodb.mongodb.commands.signatures.internal.ReplSetHeartbeatCommand.ReplSetHeartbeatArgument;
import com.torodb.mongodb.commands.signatures.internal.ReplSetUpdatePositionCommand.ReplSetUpdatePositionArgument;
import com.torodb.mongodb.commands.signatures.internal.WhatsMyUriCommand.WhatsMyUriReply;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

/**
 *
 */
public class InternalCommands implements Iterable<Command> {

  private final ImmutableList<Command> commands = ImmutableList.<Command>of(
      HandshakeCommand.INSTANCE,
      ReplSetElectCommand.INSTANCE,
      ReplSetFreshCommand.INSTANCE,
      ReplSetGetRBIDCommand.INSTANCE,
      ReplSetHeartbeatCommand.INSTANCE,
      ReplSetUpdatePositionCommand.INSTANCE,
      WhatsMyUriCommand.INSTANCE
  );

  @Override
  public Iterator<Command> iterator() {
    return commands.iterator();
  }

  @SuppressWarnings("checkstyle:LineLength")
  public abstract static class InternalCommandsImplementationsBuilder<ContextT> implements
      Iterable<Entry<Command<?, ?>, CommandImplementation<?, ?, ? super ContextT>>> {

    public abstract CommandImplementation<HandshakeArgument, Empty, ? super ContextT> getHandshakeImplementation();

    public abstract CommandImplementation<ReplSetElectArgument, ReplSetElectReply, ? super ContextT> getReplSetElectImplementation();

    public abstract CommandImplementation<ReplSetFreshArgument, ReplSetFreshReply, ? super ContextT> getReplSetFreshImplementation();

    @SuppressWarnings("checkstyle:AbbreviationAsWordInName")
    public abstract CommandImplementation<Empty, ReplSetGetRBIDReply, ? super ContextT> getReplSetGetRBIDImplementation();

    public abstract CommandImplementation<ReplSetHeartbeatArgument, ReplSetHeartbeatReply, ? super ContextT> getReplSetHeartbeatImplementation();

    public abstract CommandImplementation<ReplSetUpdatePositionArgument, Empty, ? super ContextT> getReplSetUpdateImplementation();

    public abstract CommandImplementation<Empty, WhatsMyUriReply, ContextT> getWhatsMyUriImplementation();

    private Map<Command<?, ?>, CommandImplementation<?, ?, ? super ContextT>> createMap() {
      return ImmutableMap.<Command<?, ?>, CommandImplementation<?, ?, ? super ContextT>>builder()
          .put(HandshakeCommand.INSTANCE, getHandshakeImplementation())
          .put(ReplSetElectCommand.INSTANCE, getReplSetElectImplementation())
          .put(ReplSetFreshCommand.INSTANCE, getReplSetFreshImplementation())
          .put(ReplSetGetRBIDCommand.INSTANCE, getReplSetGetRBIDImplementation())
          .put(ReplSetHeartbeatCommand.INSTANCE, getReplSetHeartbeatImplementation())
          .put(ReplSetUpdatePositionCommand.INSTANCE, getReplSetUpdateImplementation())
          .put(WhatsMyUriCommand.INSTANCE, getWhatsMyUriImplementation())
          .build();
    }

    @Override
    public Iterator<Entry<Command<?, ?>, CommandImplementation<?, ?, ? super ContextT>>> iterator() {
      return createMap().entrySet().iterator();
    }

  }

}
