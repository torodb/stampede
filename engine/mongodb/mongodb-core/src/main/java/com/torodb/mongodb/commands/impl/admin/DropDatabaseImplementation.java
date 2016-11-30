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
import com.eightkdata.mongowp.server.api.tools.Empty;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.mongodb.commands.impl.WriteTorodbCommandImpl;
import com.torodb.mongodb.core.WriteMongodTransaction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DropDatabaseImplementation implements WriteTorodbCommandImpl<Empty, Empty> {

  private static final Logger LOGGER = LogManager.getLogger(DropDatabaseImplementation.class);

  @Override
  public Status<Empty> apply(Request req, Command<? super Empty, ? super Empty> command,
      Empty arg, WriteMongodTransaction context) {
    try {
      LOGGER.info("Drop database {}", req.getDatabase());

      context.getTorodTransaction().dropDatabase(req.getDatabase());
    } catch (UserException ex) {
      //TODO: Improve error reporting
      return Status.from(ErrorCode.COMMAND_FAILED, ex.getLocalizedMessage());
    }

    return Status.ok();
  }

}
