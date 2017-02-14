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

package com.torodb.mongodb.core;

import com.eightkdata.mongowp.server.api.CommandLibrary;

/**
 * The external interface provided by a {@link MongoDbCoreBundle}.
 */
public class MongoDbCoreExtInt {

  private final MongodServer mongodServer;
  private final CommandLibrary commandLibrary;

  public MongoDbCoreExtInt(MongodServer mongodServer, CommandLibrary commandLibrary) {
    this.mongodServer = mongodServer;
    this.commandLibrary = commandLibrary;
  }

  public MongodServer getMongodServer() {
    return mongodServer;
  }

  public CommandLibrary getCommandLibrary() {
    return commandLibrary;
  }
}
