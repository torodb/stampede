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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 */
public final class NotImplementedCommandImplementation<A, R, ContextT> implements
    CommandImplementation<A, R, ContextT> {

  private static final Logger LOGGER =
      LogManager.getLogger(NotImplementedCommandImplementation.class);
  private static final NotImplementedCommandImplementation INSTANCE =
      new NotImplementedCommandImplementation();

  private NotImplementedCommandImplementation() {
  }

  public static <A, R, ContextT> NotImplementedCommandImplementation<A, R, ContextT> build() {
    return INSTANCE;
  }

  @Override
  public Status<R> apply(Request req, Command<? super A, ? super R> command, A arg,
      ContextT context) {
    LOGGER.warn("Command {} was called, but it is not supported", command.getCommandName());
    return Status.from(ErrorCode.COMMAND_NOT_SUPPORTED, "Command not supported: " + command
        .getCommandName());
  }

}
