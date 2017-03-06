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

package com.torodb.mongodb.commands.impl;

import com.eightkdata.mongowp.ErrorCode;
import com.eightkdata.mongowp.Status;
import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.CommandImplementation;
import com.eightkdata.mongowp.server.api.Request;
import com.torodb.core.logging.LoggerFactory;
import org.apache.logging.log4j.Logger;

import javax.inject.Inject;

public final class NotImplementedCommandImplementation<A, R, ContextT> implements
    CommandImplementation<A, R, ContextT> {

  private final Logger logger;

  @Inject
  public NotImplementedCommandImplementation(LoggerFactory loggerFactory) {
    logger = loggerFactory.apply(this.getClass());
  }

  @Override
  public Status<R> apply(Request req, Command<? super A, ? super R> command, A arg,
      ContextT context) {
    logger.warn("Command {} was called, but it is not supported", command.getCommandName());
    return Status.from(ErrorCode.COMMAND_NOT_SUPPORTED, "Command not supported: " + command
        .getCommandName());
  }

}
