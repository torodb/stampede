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
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.logging.LoggerFactory;
import com.torodb.torod.SharedWriteTorodTransaction;
import org.apache.logging.log4j.Logger;

import javax.inject.Inject;

public class DropDatabaseReplImpl extends ReplCommandImpl<Empty, Empty> {

  private final Logger logger;
  private final CommandFilterUtil filterUtil;

  @Inject
  public DropDatabaseReplImpl(CommandFilterUtil filterUtil, LoggerFactory loggerFactory) {
    this.logger = loggerFactory.apply(this.getClass());
    this.filterUtil = filterUtil;
  }

  @Override
  public Status<Empty> apply(Request req,
      Command<? super Empty, ? super Empty> command, Empty arg,
      SharedWriteTorodTransaction trans) {

    if (!filterUtil.testDbFilter(req.getDatabase(), command)) {
      return Status.ok();
    }
    
    try {
      logger.info("Dropping database {}", req.getDatabase());

      if (trans.existsDatabase(req.getDatabase())) {
        trans.dropDatabase(req.getDatabase());
      } else {
        logger.info("Trying to drop database " + req.getDatabase() + " but it has not been found. "
            + "This is normal since the database could have a collection being filtered "
            + "or we are reapplying oplog during a recovery. Ignoring operation");
      }
    } catch (UserException ex) {
      reportErrorIgnored(logger, command, ex);
    }

    return Status.ok();
  }

}
