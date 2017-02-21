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
import com.eightkdata.mongowp.server.api.impl.CollectionCommandArgument;
import com.eightkdata.mongowp.server.api.tools.Empty;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.torod.SharedWriteTorodTransaction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.inject.Inject;

public class DropCollectionReplImpl extends ReplCommandImpl<CollectionCommandArgument, Empty> {

  private static final Logger LOGGER = LogManager.getLogger(DropCollectionReplImpl.class);

  private final CommandFilterUtil filterUtil;

  @Inject
  public DropCollectionReplImpl(CommandFilterUtil filterUtil) {
    this.filterUtil = filterUtil;
  }

  @Override
  public Status<Empty> apply(
      Request req,
      Command<? super CollectionCommandArgument, ? super Empty> command,
      CollectionCommandArgument arg,
      SharedWriteTorodTransaction trans) {

    if (!filterUtil.testNamespaceFilter(req.getDatabase(), arg.getCollection(), command)) {
      return Status.ok();
    }

    try {
      LOGGER.info("Dropping collection {}.{}", req.getDatabase(), arg.getCollection());

      if (trans.existsCollection(req.getDatabase(), arg.getCollection())) {
        trans.dropCollection(req.getDatabase(), arg.getCollection());
      } else {
        LOGGER.info("Trying to drop collection {}.{} but it has not been found. "
            + "This is normal when reapplying oplog during a recovery. Ignoring operation",
            req.getDatabase(), arg.getCollection());
      }
    } catch (UserException ex) {
      reportErrorIgnored(LOGGER, command, ex);
    }

    return Status.ok();

  }

}
