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

package com.torodb.core.logging;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.MessageFactory;
import org.apache.logging.log4j.spi.ExtendedLogger;

public interface MessageLoggerFactory extends LoggerFactory {

  public MessageFactory getMessageFactory();

  public String getUniqueLoggerName(Class<?> clazz);

  @Override
  public default Logger apply(Class<?> clazz) {
    MessageFactory messageFactory = getMessageFactory();
    Logger decorateLogger = LogManager.getLogger(clazz);
    if (decorateLogger instanceof ExtendedLogger) {
      return new DecoratorLogger(
          decorateLogger.getName(),
          (ExtendedLogger) decorateLogger,
          messageFactory
      );
    }
    String uniqueLoggerName = getUniqueLoggerName(clazz);
    if (uniqueLoggerName.equals(clazz.getName())) {
      throw new AssertionError("The unique name for a class must not be the same as the class "
          + "name");
    }
    return LogManager.getLogger(uniqueLoggerName, messageFactory);
  }

}
