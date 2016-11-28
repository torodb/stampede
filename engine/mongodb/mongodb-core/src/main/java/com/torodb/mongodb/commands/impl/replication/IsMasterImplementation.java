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

package com.torodb.mongodb.commands.impl.replication;

import com.eightkdata.mongowp.Status;
import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.Request;
import com.eightkdata.mongowp.server.api.tools.Empty;
import com.torodb.mongodb.commands.impl.ConnectionTorodbCommandImpl;
import com.torodb.mongodb.commands.signatures.repl.IsMasterCommand.IsMasterReply;
import com.torodb.mongodb.core.MongoLayerConstants;
import com.torodb.mongodb.core.MongodConnection;
import com.torodb.mongodb.core.MongodServerConfig;

import java.time.Clock;

import javax.inject.Inject;
import javax.inject.Singleton;

/**
 *
 */
@Singleton
public class IsMasterImplementation extends ConnectionTorodbCommandImpl<Empty, IsMasterReply> {

  private final Clock clock;

  @Inject
  public IsMasterImplementation(Clock clock, MongodServerConfig ourConfig) {
    this.clock = clock;
  }

  @Override
  public Status<IsMasterReply> apply(Request req,
      Command<? super Empty, ? super IsMasterReply> command, Empty arg, MongodConnection context) {
    return Status.ok(
        IsMasterReply.Builder.fromStandalone(
            MongoLayerConstants.MAX_BSON_DOCUMENT_SIZE,
            MongoLayerConstants.MAX_MESSAGE_SIZE_BYTES,
            MongoLayerConstants.MAX_WRITE_BATCH_SIZE,
            clock.instant(),
            MongoLayerConstants.MAX_WIRE_VERSION,
            MongoLayerConstants.MIN_WIRE_VERSION
        ).build()
    );

  }
}
