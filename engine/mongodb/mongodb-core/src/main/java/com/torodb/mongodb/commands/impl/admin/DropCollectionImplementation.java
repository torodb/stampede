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

package com.torodb.mongodb.commands.impl.admin;

import com.eightkdata.mongowp.ErrorCode;
import com.eightkdata.mongowp.Status;
import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.Request;
import com.eightkdata.mongowp.server.api.impl.CollectionCommandArgument;
import com.eightkdata.mongowp.server.api.tools.Empty;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.logging.LoggerFactory;
import com.torodb.mongodb.commands.impl.WriteTorodbCommandImpl;
import com.torodb.mongodb.core.WriteMongodTransaction;
import org.apache.logging.log4j.Logger;

import javax.inject.Inject;

public class DropCollectionImplementation implements
    WriteTorodbCommandImpl<CollectionCommandArgument, Empty> {

  private final Logger logger;

  @Inject
  public DropCollectionImplementation(LoggerFactory loggerFactory) {
    this.logger = loggerFactory.apply(this.getClass());
  }

  @Override
  public Status<Empty> apply(Request req,
      Command<? super CollectionCommandArgument, ? super Empty> command,
      CollectionCommandArgument arg, WriteMongodTransaction context) {
    try {
      logDropCommand(arg);

      context.getTorodTransaction().dropCollection(req.getDatabase(), arg.getCollection());
    } catch (UserException ex) {
      //TODO: Improve error reporting
      return Status.from(ErrorCode.COMMAND_FAILED, ex.getLocalizedMessage());
    }

    return Status.ok();
  }

  private void logDropCommand(CollectionCommandArgument arg) {
    String collection = arg.getCollection();

    logger.info("Drop collection {}", collection);
  }

}
