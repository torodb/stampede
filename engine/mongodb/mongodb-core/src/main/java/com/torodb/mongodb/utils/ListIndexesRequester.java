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

package com.torodb.mongodb.utils;

import com.eightkdata.mongowp.MongoVersion;
import com.eightkdata.mongowp.client.core.MongoConnection;
import com.eightkdata.mongowp.client.core.MongoConnection.RemoteCommandResponse;
import com.eightkdata.mongowp.exceptions.MongoException;
import com.torodb.mongodb.commands.pojos.CursorResult;
import com.torodb.mongodb.commands.pojos.index.IndexOptions;
import com.torodb.mongodb.commands.signatures.admin.ListIndexesCommand;
import com.torodb.mongodb.commands.signatures.admin.ListIndexesCommand.ListIndexesResult;

/**
 * Utility class to get the indexes metadata in a version independient way.
 * <p/>
 * The command {@link ListIndexesCommand listIndexes} is only available since
 * {@linkplain MongoVersion#V3_0 MongoDB 3.0}. In previous versions, a query to an specific
 * metacollection must be done.
 * <p/>
 * This class is used request for collections metadata in a version independient way.
 */
public class ListIndexesRequester {

  private ListIndexesRequester() {
  }

  public static CursorResult<IndexOptions> getListCollections(
      MongoConnection connection,
      String database,
      String collection
  ) throws MongoException {
    boolean commandSupported = connection.getClientOwner()
        .getMongoVersion().compareTo(MongoVersion.V3_0) >= 0;
    if (commandSupported) {
      return getFromCommand(connection, database, collection);
    } else {
      return getFromQuery(connection, database, collection);
    }
  }

  private static CursorResult<IndexOptions> getFromCommand(
      MongoConnection connection,
      String database,
      String collection) throws MongoException {
    RemoteCommandResponse<ListIndexesResult> reply = connection.execute(
        ListIndexesCommand.INSTANCE,
        database,
        true,
        new ListIndexesCommand.ListIndexesArgument(
            collection
        )
    );
    if (!reply.isOk()) {
      throw reply.asMongoException();
    }
    return reply.getCommandReply().get().getCursor();
  }

  private static CursorResult<IndexOptions> getFromQuery(
      MongoConnection connection,
      String database,
      String collection) {
    throw new UnsupportedOperationException("Not supported yet");
  }
}
