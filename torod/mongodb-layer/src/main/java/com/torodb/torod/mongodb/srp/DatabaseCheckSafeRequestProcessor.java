
package com.torodb.torod.mongodb.srp;

import com.eightkdata.mongowp.messages.request.DeleteMessage;
import com.eightkdata.mongowp.messages.request.InsertMessage;
import com.eightkdata.mongowp.messages.request.UpdateMessage;
import com.eightkdata.mongowp.messages.response.ReplyMessage;
import com.eightkdata.mongowp.mongoserver.api.safe.*;
import com.eightkdata.mongowp.mongoserver.api.safe.impl.UpdateOpResult;
import com.eightkdata.mongowp.mongoserver.api.safe.pojos.QueryRequest;
import com.eightkdata.mongowp.mongoserver.callback.WriteOpResult;
import com.eightkdata.mongowp.mongoserver.protocol.exceptions.CommandNotSupportedException;
import com.eightkdata.mongowp.mongoserver.protocol.exceptions.DatabaseNotFoundException;
import com.eightkdata.mongowp.mongoserver.protocol.exceptions.MongoException;
import com.google.common.util.concurrent.ListenableFuture;
import com.torodb.torod.core.annotations.DatabaseName;
import com.torodb.torod.mongodb.annotations.External;
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

    @Override
    public <Arg, Result> CommandReply<Result> execute(Command<? super Arg, ? super Result> command, CommandRequest<Arg> request)
            throws MongoException, CommandNotSupportedException {
        String database = request.getDatabase();
        assert database != null;
        checkDatabase(database);
        return super.execute(command, request);
    }
}
