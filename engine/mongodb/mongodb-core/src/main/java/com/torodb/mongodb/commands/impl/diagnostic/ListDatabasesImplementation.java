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

package com.torodb.mongodb.commands.impl.diagnostic;

import com.eightkdata.mongowp.Status;
import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.Request;
import com.eightkdata.mongowp.server.api.tools.Empty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.torodb.mongodb.commands.impl.ReadTorodbCommandImpl;
import com.torodb.mongodb.commands.signatures.diagnostic.ListDatabasesCommand.ListDatabasesReply;
import com.torodb.mongodb.commands.signatures.diagnostic.ListDatabasesCommand.ListDatabasesReply.DatabaseEntry;
import com.torodb.mongodb.core.MongodTransaction;

import java.util.List;

import javax.inject.Singleton;

/**
 *
 */
@Singleton
public class ListDatabasesImplementation
    implements ReadTorodbCommandImpl<Empty, ListDatabasesReply> {

  @Override
  public Status<ListDatabasesReply> apply(Request req,
      Command<? super Empty, ? super ListDatabasesReply> command,
      Empty arg, MongodTransaction context) {
    List<String> databases = context.getTorodTransaction().getDatabases();

    long totalSize = 0;
    List<DatabaseEntry> databaseEntries = Lists.newArrayListWithCapacity(databases.size());

    for (String databaseName : databases) {
      long databaseSize = context.getTorodTransaction().getDatabaseSize(databaseName);
      databaseEntries.add(
          new DatabaseEntry(
              databaseName,
              databaseSize,
              databaseSize == 0)
      );
      totalSize += databaseSize;
    }
    return Status.ok(new ListDatabasesReply(ImmutableList.copyOf(databaseEntries), totalSize));
  }

}
