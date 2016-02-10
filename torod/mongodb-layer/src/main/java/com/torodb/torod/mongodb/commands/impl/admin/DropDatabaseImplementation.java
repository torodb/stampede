
package com.torodb.torod.mongodb.commands.impl.admin;

import com.eightkdata.mongowp.exceptions.CommandFailed;
import com.eightkdata.mongowp.exceptions.InternalErrorException;
import com.eightkdata.mongowp.exceptions.MongoException;
import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.CommandRequest;
import com.eightkdata.mongowp.server.api.CommandResult;
import com.eightkdata.mongowp.server.api.impl.NonWriteCommandResult;
import com.eightkdata.mongowp.server.api.tools.Empty;
import com.torodb.torod.core.annotations.DatabaseName;
import com.torodb.torod.core.connection.ToroConnection;
import com.torodb.torod.core.exceptions.ClosedToroCursorException;
import com.torodb.torod.core.exceptions.ToroException;
import com.torodb.torod.core.exceptions.UserToroException;
import com.torodb.torod.core.pojos.CollectionMetainfo;
import com.torodb.torod.mongodb.commands.AbstractToroCommandImplementation;
import com.torodb.torod.mongodb.utils.NamespaceUtil;
import javax.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class DropDatabaseImplementation extends AbstractToroCommandImplementation<Empty, Empty>{

    private static final Logger LOGGER
            = LoggerFactory.getLogger(DropDatabaseImplementation.class);

    private final String supporteDatabase;

    @Inject
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

                for (CollectionMetainfo collectionMetainfo : connection.getCollectionsMetainfoCursor()) {
                    if (!NamespaceUtil.isTorodbCollection(collectionMetainfo.getName())) {
                        connection.dropCollection(collectionMetainfo.getName());
                    }
                }
            } catch (ClosedToroCursorException | UserToroException ex) {
                throw new CommandFailed(command.getCommandName(), ex.getMessage(), ex);
            } catch (ToroException ex) {
                throw new InternalErrorException(ex);
            }
            return new NonWriteCommandResult<>(Empty.getInstance());
        }
    }

}
