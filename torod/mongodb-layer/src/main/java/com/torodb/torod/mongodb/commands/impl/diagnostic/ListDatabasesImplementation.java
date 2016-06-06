
package com.torodb.torod.mongodb.commands.impl.diagnostic;

import com.eightkdata.mongowp.exceptions.CommandFailed;
import com.eightkdata.mongowp.exceptions.MongoException;
import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.CommandRequest;
import com.eightkdata.mongowp.server.api.CommandResult;
import com.eightkdata.mongowp.server.api.impl.NonWriteCommandResult;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.diagnostic.ListDatabasesCommand.ListDatabasesReply;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.diagnostic.ListDatabasesCommand.ListDatabasesReply.DatabaseEntry;
import com.eightkdata.mongowp.server.api.tools.Empty;
import com.google.common.collect.Lists;
import com.torodb.torod.core.connection.ToroConnection;
import com.torodb.torod.core.connection.ToroTransaction;
import com.torodb.torod.core.connection.TransactionMetainfo;
import com.torodb.torod.core.dbWrapper.exceptions.ImplementationDbException;
import com.torodb.torod.core.pojos.Database;
import com.torodb.torod.mongodb.commands.AbstractToroCommandImplementation;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 *
 */
public class ListDatabasesImplementation extends AbstractToroCommandImplementation<Empty, ListDatabasesReply>{

    public static final ListDatabasesImplementation INSTANCE = new ListDatabasesImplementation();

    private ListDatabasesImplementation() {
    }

    @Override
    public CommandResult<ListDatabasesReply> apply(Command<? super Empty, ? super ListDatabasesReply> command, CommandRequest<Empty> req)
            throws MongoException {
        try {
            ToroConnection connection = getToroConnection(req);

            try (ToroTransaction transaction = connection.createTransaction(TransactionMetainfo.READ_ONLY)) {
                List<? extends Database> databases = transaction.getDatabases().get();
                
                long totalSize = 0;
                List<DatabaseEntry> databaseEntries = Lists.newArrayListWithCapacity(databases.size());

                for (Database database : databases) {
                    databaseEntries.add(
                            new DatabaseEntry(
                                    database.getName(),
                                    database.getSize(),
                                    database.getSize() == 0)
                    );
                    totalSize += database.getSize();
                }
                return new NonWriteCommandResult<>(
                        new ListDatabasesReply(
                                databaseEntries,
                                totalSize
                        )
                );
            } catch (InterruptedException ex) {
                throw new CommandFailed(command.getCommandName(), ex.getMessage(), ex);
            } catch (ExecutionException ex) {
                throw new CommandFailed(command.getCommandName(), ex.getMessage(), ex);
            }
            
        } catch (ImplementationDbException ex) {
            throw new CommandFailed(command.getCommandName(), ex.getMessage(), ex);
        }

    }

}
