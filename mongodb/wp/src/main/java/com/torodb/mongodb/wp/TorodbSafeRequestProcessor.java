
package com.torodb.mongodb.wp;

import com.codahale.metrics.Timer;
import com.eightkdata.mongowp.ErrorCode;
import com.eightkdata.mongowp.Status;
import com.eightkdata.mongowp.bson.utils.DefaultBsonValues;
import com.eightkdata.mongowp.exceptions.MongoException;
import com.eightkdata.mongowp.messages.request.*;
import com.eightkdata.mongowp.messages.response.ReplyMessage;
import com.eightkdata.mongowp.messages.utils.IterableDocumentProvider;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.FindCommand;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.FindCommand.FindArgument;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.FindCommand.FindResult;
import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.CommandsLibrary;
import com.eightkdata.mongowp.server.api.Request;
import com.eightkdata.mongowp.server.api.SafeRequestProcessor;
import com.eightkdata.mongowp.server.api.pojos.QueryRequest;
import com.google.common.collect.Lists;
import com.torodb.core.retrier.Retrier;
import com.torodb.core.retrier.RetrierGiveUpException;
import com.torodb.mongodb.commands.TorodbCommandsLibrary;
import com.torodb.mongodb.commands.TorodbCommandsLibrary.RequiredTransaction;
import com.torodb.mongodb.core.*;
import io.netty.util.AttributeKey;
import java.util.concurrent.Callable;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 */
@Singleton
public class TorodbSafeRequestProcessor implements SafeRequestProcessor<MongodConnection> {

    private static final Logger LOGGER = LogManager.getLogger(TorodbSafeRequestProcessor.class);
    private final MongodServer server;
    public static final AttributeKey<MongodConnection> MONGOD_CONNECTION_KEY = AttributeKey.newInstance("mongod.connection");
    private final Retrier retrier;
    private final TorodbCommandsLibrary commandsLibrary;
    private final MongodMetrics mongodMetrics;

    @Inject
    public TorodbSafeRequestProcessor(MongodServer server, Retrier retrier, TorodbCommandsLibrary commandsLibrary, MongodMetrics mongodMetrics) {
        this.server = server;
        this.retrier = retrier;
        this.commandsLibrary = commandsLibrary;
        this.mongodMetrics = mongodMetrics;
    }

    @Override
    public MongodConnection openConnection() {
        MongodConnection connection = server.openConnection();

        LOGGER.info("Accepted connection {}", connection.getConnectionId());

        return connection;
    }

    @Override
    public <Arg, Result> Status<Result> execute(Request req, Command<? super Arg, ? super Result> command, Arg arg, MongodConnection connection) {
    	mongodMetrics.getCommands().mark();
    	Timer timer = mongodMetrics.getTimer(command);
    	try (Timer.Context ctx = timer.time()) {
	    	Callable<Status<Result>> callable;
	        RequiredTransaction commandType = commandsLibrary.getCommandType(command);
	        switch (commandType) {
	            case NO_TRANSACTION:
	                callable = () -> {
	                    return connection.getCommandsExecutor().execute(req, command, arg, connection);
	                };
	                break;
	            case READ_TRANSACTION:
	                callable = () -> {
	                    try (ReadOnlyMongodTransaction trans = connection.openReadOnlyTransaction()) {
	                        return trans.execute(req, command, arg);
	                    }
	                };
	                break;
                case WRITE_TRANSACTION:
                    callable = () -> {
                        try (WriteMongodTransaction trans = connection.openWriteTransaction(true)) {
                            Status<Result> result = trans.execute(req, command, arg);
                            trans.commit();
                            return result;
                        }
                    };
                    break;
                case EXCLUSIVE_WRITE_TRANSACTION:
                    callable = () -> {
                        try (ExclusiveWriteMongodTransaction trans = connection.openExclusiveWriteTransaction(true)) {
                            Status<Result> result = trans.execute(req, command, arg);
                            trans.commit();
                            return result;
                        }
                    };
                    break;
	            default:
	                throw new AssertionError("Unexpected command type" + commandType);
	        }

            try {
                return retrier.retry(callable);
            } catch (RetrierGiveUpException ex) {
                return Status.from(
                        ErrorCode.CONFLICTING_OPERATION_IN_PROGRESS,
                        "It was impossible to execute " + command.getCommandName() + " after several attempts"
                );
            }
        }
    }

    @Override
    public CommandsLibrary getCommandsLibrary() {
        return commandsLibrary;
    }

    @Override
    public ReplyMessage query(MongodConnection connection, Request req, int requestId, QueryRequest queryRequest) throws
            MongoException {

        FindArgument findArg = new FindArgument.Builder()
                .setCollection(queryRequest.getCollection())
                .setFilter(queryRequest.getQuery() != null ? queryRequest.getQuery() : DefaultBsonValues.EMPTY_DOC)
                .build();

        Status<FindResult> status = execute(req, FindCommand.INSTANCE, findArg, connection);

        if (!status.isOk()) {
            throw new MongoException(status.getErrorCode(), status.getErrorMsg());
        }

        FindResult result = status.getResult();
        assert result != null;

        return new ReplyMessage(
                EmptyBsonContext.getInstance(),
                requestId,
                false,
                false,
                false,
                false,
                result.getCursor().getCursorId(),
                queryRequest.getNumberToSkip(),
                IterableDocumentProvider.of(Lists.newArrayList(result.getCursor().getFirstBatch()))
        );
    }

    @Override
    public ReplyMessage getMore(MongodConnection connection, Request req, int requestId,GetMoreMessage moreMessage)
            throws MongoException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void killCursors(MongodConnection connection, Request req, KillCursorsMessage killCursorsMessage)
            throws MongoException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void insert(MongodConnection connection, Request req, InsertMessage insertMessage) throws
            MongoException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void update(MongodConnection connection, Request req, UpdateMessage updateMessage) throws
            MongoException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void delete(MongodConnection connection, Request req, DeleteMessage deleteMessage) throws
            MongoException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

}
