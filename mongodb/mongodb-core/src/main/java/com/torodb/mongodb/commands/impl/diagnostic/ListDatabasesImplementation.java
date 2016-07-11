
package com.torodb.mongodb.commands.impl.diagnostic;

import java.util.List;

import javax.inject.Singleton;

import com.eightkdata.mongowp.Status;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.diagnostic.ListDatabasesCommand.ListDatabasesReply;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.diagnostic.ListDatabasesCommand.ListDatabasesReply.DatabaseEntry;
import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.Request;
import com.eightkdata.mongowp.server.api.tools.Empty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.torodb.mongodb.commands.impl.ReadTorodbCommandImpl;
import com.torodb.mongodb.core.MongodTransaction;

/**
 *
 */
@Singleton
public class ListDatabasesImplementation implements ReadTorodbCommandImpl<Empty, ListDatabasesReply> {

    @Override
    public Status<ListDatabasesReply> apply(Request req, Command<? super Empty, ? super ListDatabasesReply> command,
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
