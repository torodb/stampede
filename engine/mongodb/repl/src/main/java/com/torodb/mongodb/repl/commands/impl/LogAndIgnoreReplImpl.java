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

package com.torodb.mongodb.repl.commands.impl;

import com.eightkdata.mongowp.Status;
import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.Request;
import com.eightkdata.mongowp.server.api.tools.Empty;
import com.torodb.core.logging.LoggerFactory;
import com.torodb.torod.SharedWriteTorodTransaction;
import org.apache.logging.log4j.Logger;

import javax.inject.Inject;

public class LogAndIgnoreReplImpl extends ReplCommandImpl<String, Empty> {

  private final Logger logger;
  private final CommandFilterUtil filterUtil;

  @Inject
  public LogAndIgnoreReplImpl(CommandFilterUtil filterUtil, LoggerFactory loggerFactory) {
    this.filterUtil = filterUtil;
    this.logger = loggerFactory.apply(this.getClass());
  }

  @Override
  public Status<Empty> apply(Request req,
      Command<? super String, ? super Empty> command,
      String arg, SharedWriteTorodTransaction trans) {
    if (!filterUtil.testDbFilter(req.getDatabase(), command)) {
      return Status.ok();
    }

    logger.warn("Command {} is not supported. It will not be executed", arg);
    return Status.ok();
  }

}
