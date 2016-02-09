
package com.torodb.torod.mongodb.srp;

import com.eightkdata.mongowp.server.api.CommandReply;
import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.CommandRequest;
import com.eightkdata.mongowp.server.api.Request;
import com.eightkdata.mongowp.exceptions.CommandNotSupportedException;
import com.eightkdata.mongowp.exceptions.DatabaseNotFoundException;
import com.eightkdata.mongowp.exceptions.MongoException;
import com.eightkdata.mongowp.messages.request.DeleteMessage;
import com.eightkdata.mongowp.messages.request.InsertMessage;
import com.eightkdata.mongowp.messages.request.UpdateMessage;
import com.eightkdata.mongowp.messages.response.ReplyMessage;
import com.eightkdata.mongowp.server.api.impl.UpdateOpResult;
import com.eightkdata.mongowp.server.api.pojos.QueryRequest;
import com.eightkdata.mongowp.server.callback.WriteOpResult;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import com.torodb.torod.core.annotations.DatabaseName;
import com.torodb.torod.mongodb.annotations.External;
import java.util.Locale;
import javax.inject.Inject;
import javax.inject.Singleton;

/**
 * This {@linkplain SafeRequestProcessor} enforces the database restriction.
 * <p/>
 * Right now, ToroDB only supports one database whose name is decided when the
 * service start. External requests can only be accepted if they are executed
 * on:
 * <ul>
 * <li>The supported database</li>
 * <li>The <em>admin</em> database</li>
 * <li>The <em>local</em> database</li>
 * </ul>
 */
@Singleton @External
public class DatabaseCheckSafeRequestProcessor extends DecoratorSafeRequestProcessor {
    private static final String ADMIN_DB = "admin";
    private static final String LOCAL_DB = "local";
    /**
     * The lower case name of commands that can bypass the database check.
     */
    private static final ImmutableSet<String> WHITE_COMMAND_LIST = ImmutableSet.of("ismaster");

    private final String supportedDatabase;

    @Inject
    public DatabaseCheckSafeRequestProcessor(ReplicationCheckSafeRequestProcessor delegate, @DatabaseName String supportedDatabase) {
        super(delegate);
        this.supportedDatabase = supportedDatabase;
    }

    private boolean isAllowed(String database) {
        assert database != null : "only requests with database should be catched by this decorator";
        return database.equals(supportedDatabase) 
                || database.equals(ADMIN_DB)
                || database.equals(LOCAL_DB);
    }

    private void checkDatabase(String database) throws DatabaseNotFoundException {
        if (!isAllowed(database)) {
            throw new DatabaseNotFoundException(
                    database,
                    "Database '" + database + "' is not supported. "
                    + "Only '" + supportedDatabase +"' is supported");
        }
    }

    @Override
    public ListenableFuture<? extends WriteOpResult> delete(Request request, DeleteMessage deleteMessage)
            throws MongoException {
        String database = deleteMessage.getDatabase();
        assert database != null;
        assert database.equals(request.getDatabase());
        checkDatabase(database);
        return super.delete(request, deleteMessage);
    }

    @Override
    public ListenableFuture<? extends UpdateOpResult> update(Request request, UpdateMessage update)
            throws MongoException {
        String database = update.getDatabase();
        assert database != null;
        assert database.equals(request.getDatabase());
        checkDatabase(database);
        return super.update(request, update);
    }

    @Override
    public ListenableFuture<? extends WriteOpResult> insert(Request request, InsertMessage insertMessage)
            throws MongoException {
        String database = insertMessage.getDatabase();
        assert database != null;
        assert database.equals(request.getDatabase());
        checkDatabase(database);
        return super.insert(request, insertMessage);
    }

    @Override
    public ReplyMessage query(Request request, QueryRequest queryMessage) throws
            MongoException {
        String database = queryMessage.getDatabase();
        assert database != null;
        assert database.equals(request.getDatabase());
        checkDatabase(database);
        return super.query(request, queryMessage);
    }

    private <Arg, Result>  boolean onWhiteList(Command<? super Arg, ? super Result> command) {
        return WHITE_COMMAND_LIST.contains(command.getCommandName().toLowerCase(Locale.ENGLISH));
    }

    @Override
    public <Arg, Result> CommandReply<Result> execute(Command<? super Arg, ? super Result> command, CommandRequest<Arg> request)
            throws MongoException, CommandNotSupportedException {
        if (!onWhiteList(command)) {
            String database = request.getDatabase();
            assert database != null;
            checkDatabase(database);
        }
        return super.execute(command, request);
    }
}
