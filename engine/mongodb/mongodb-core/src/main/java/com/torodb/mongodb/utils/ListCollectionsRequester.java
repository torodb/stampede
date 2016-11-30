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
import com.eightkdata.mongowp.bson.BsonDocument;
import com.eightkdata.mongowp.client.core.MongoConnection;
import com.eightkdata.mongowp.client.core.MongoConnection.RemoteCommandResponse;
import com.eightkdata.mongowp.exceptions.MongoException;
import com.torodb.mongodb.commands.pojos.CursorResult;
import com.torodb.mongodb.commands.signatures.admin.ListCollectionsCommand;
import com.torodb.mongodb.commands.signatures.admin.ListCollectionsCommand.ListCollectionsResult;
import com.torodb.mongodb.commands.signatures.admin.ListCollectionsCommand.ListCollectionsResult.Entry;

import javax.annotation.Nullable;

/**
 * Utility class to get the collections metadata in a version independient way.
 * <p/>
 * The command {@link ListCollectionsCommand listCollections} is only available since
 * {@linkplain MongoVersion#V3_0 MongoDB 3.0}. In previous versions, a query to an specific
 * metacollection must be done.
 * <p/>
 * This class is used request for collections metadata in a version independient way.
 */
public class ListCollectionsRequester {

  private ListCollectionsRequester() {
  }

  public static CursorResult<Entry> getListCollections(
      MongoConnection connection,
      String database,
      @Nullable BsonDocument filter
  ) throws MongoException {
    boolean commandSupported = connection.getClientOwner()
        .getMongoVersion().compareTo(MongoVersion.V3_0) >= 0;
    if (commandSupported) {
      return getFromCommand(connection, database, filter);
    } else {
      return getFromQuery(connection, database, filter);
    }
  }

  private static CursorResult<Entry> getFromCommand(
      MongoConnection connection,
      String database,
      @Nullable BsonDocument filter
  ) throws MongoException {
    RemoteCommandResponse<ListCollectionsResult> reply = connection.execute(
        ListCollectionsCommand.INSTANCE,
        database,
        true,
        new ListCollectionsCommand.ListCollectionsArgument(
            filter
        )
    );
    if (!reply.isOk()) {
      throw reply.asMongoException();
    }
    return reply.getCommandReply().get().getCursor();
  }

  private static CursorResult<Entry> getFromQuery(
      MongoConnection connection,
      String database,
      BsonDocument filter) {
    throw new UnsupportedOperationException("Not supported yet");
  }

}
