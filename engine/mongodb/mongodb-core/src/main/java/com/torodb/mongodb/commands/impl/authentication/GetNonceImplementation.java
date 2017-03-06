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

package com.torodb.mongodb.commands.impl.authentication;

import com.eightkdata.mongowp.Status;
import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.Request;
import com.eightkdata.mongowp.server.api.tools.Empty;
import com.torodb.core.logging.LoggerFactory;
import com.torodb.mongodb.commands.impl.ConnectionTorodbCommandImpl;
import com.torodb.mongodb.core.MongodConnection;
import org.apache.logging.log4j.Logger;

import java.util.Random;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

@ThreadSafe
public class GetNonceImplementation extends ConnectionTorodbCommandImpl<Empty, String> {

  private final Logger logger;
  private final Random random = new Random();

  @Inject
  public GetNonceImplementation(LoggerFactory loggerFactory) {
    this.logger = loggerFactory.apply(this.getClass());
  }

  @Override
  public Status<String> apply(
      Request req,
      Command<? super Empty, ? super String> command,
      Empty arg,
      MongodConnection context) {
    logger.warn("Authentication not supported. Operation 'getnonce' called. A fake value is "
        + "returned");

    String nonce = Long.toHexString(random.nextLong());

    return Status.ok(nonce);
  }

}
