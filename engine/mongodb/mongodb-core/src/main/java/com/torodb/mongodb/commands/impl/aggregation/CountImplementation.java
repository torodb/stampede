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

package com.torodb.mongodb.commands.impl.aggregation;

import com.eightkdata.mongowp.Status;
import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.Request;
import com.torodb.mongodb.commands.impl.ReadTorodbCommandImpl;
import com.torodb.mongodb.commands.signatures.aggregation.CountCommand.CountArgument;
import com.torodb.mongodb.core.MongodTransaction;

import javax.inject.Singleton;

/**
 *
 */
@Singleton
public class CountImplementation implements ReadTorodbCommandImpl<CountArgument, Long> {

  @Override
  public Status<Long> apply(Request req, Command<? super CountArgument, ? super Long> command,
      CountArgument arg, MongodTransaction context) {
    return Status.ok(
        context.getTorodTransaction()
            .countAll(req.getDatabase(), arg.getCollection())
    );
  }

}
