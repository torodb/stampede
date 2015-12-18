
package com.torodb.torod.mongodb.commands.impl.admin;

import com.eightkdata.mongowp.mongoserver.api.safe.Command;
import com.eightkdata.mongowp.mongoserver.api.safe.CommandRequest;
import com.eightkdata.mongowp.mongoserver.api.safe.CommandResult;
import com.eightkdata.mongowp.mongoserver.api.safe.impl.NonWriteCommandResult;
import com.eightkdata.mongowp.mongoserver.api.safe.tools.Empty;
import com.eightkdata.mongowp.mongoserver.protocol.exceptions.CommandFailed;
import com.eightkdata.mongowp.mongoserver.protocol.exceptions.MongoException;
import com.torodb.torod.core.annotations.DatabaseName;
import com.torodb.torod.core.connection.ToroConnection;
import com.torodb.torod.core.cursors.UserCursor;
import com.torodb.torod.core.exceptions.ClosedToroCursorException;
import com.torodb.torod.core.pojos.CollectionMetainfo;
import com.torodb.torod.mongodb.commands.AbstractToroCommandImplementation;
import com.torodb.torod.mongodb.utils.NamespaceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class DropDatabaseImplementation extends AbstractToroCommandImplementation<Empty, Empty>{

    private static final Logger LOGGER
            = LoggerFactory.getLogger(DropDatabaseImplementation.class);

    private final String supporteDatabase;

    public DropDatabaseImplementation(@DatabaseName String supporteDatabase) {
        this.supporteDatabase = supporteDatabase;
    }

    @Override
    public CommandResult<Empty> apply(Command<? super Empty, ? super Empty> command, CommandRequest<Empty> req)
            throws MongoException {
        String database = req.getDatabase();

        if (!supporteDatabase.equals(database)) {
            LOGGER.warn("Ignoring drop database command with database {}. Only {} is supported", database, supporteDatabase);
            return new NonWriteCommandResult<Empty>(Empty.getInstance());
        }
        else {
            try {
                ToroConnection connection = getToroConnection(req);
                UserCursor<CollectionMetainfo> cursor = connection.openCollectionsMetainfoCursor();

                for (CollectionMetainfo collectionMetainfo : cursor.readAll()) {
                    if (!NamespaceUtil.isTorodbCollection(collectionMetainfo.getName())) {
                        connection.dropCollection(collectionMetainfo.getName());
                    }
                }
            } catch (ClosedToroCursorException ex) {
                throw new CommandFailed(command.getCommandName(), ex.getMessage(), ex);
            }
            return new NonWriteCommandResult<Empty>(Empty.getInstance());
        }
    }

}
