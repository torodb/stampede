
package com.torodb.mongodb.mongowp;

import com.eightkdata.mongowp.Status;
import com.eightkdata.mongowp.exceptions.CommandNotSupportedException;
import com.eightkdata.mongowp.exceptions.MongoException;
import com.eightkdata.mongowp.messages.request.*;
import com.eightkdata.mongowp.messages.response.ReplyMessage;
import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.CommandsLibrary;
import com.eightkdata.mongowp.server.api.Request;
import com.eightkdata.mongowp.server.api.SafeRequestProcessor;
import com.eightkdata.mongowp.server.api.pojos.QueryRequest;
import com.torodb.mongodb.core.MongodConnection;
import com.torodb.mongodb.core.MongodServer;
import com.torodb.mongodb.core.MongodTransaction;
import io.netty.util.AttributeKey;
import javax.inject.Inject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 */
public class TorodbSafeRequestProcessor implements SafeRequestProcessor<MongodConnection> {

    private static final Logger LOGGER = LogManager.getLogger(TorodbSafeRequestProcessor.class);
    private final MongodServer server;
    public static final AttributeKey<MongodConnection> MONGOD_CONNECTION_KEY = AttributeKey.newInstance("mongod.connection");

    @Inject
    public TorodbSafeRequestProcessor(MongodServer server) {
        this.server = server;
    }

    @Override
    public MongodConnection openConnection() {
        return server.openConnection();
    }

    @Override
    public <Arg, Result> Status<Result> execute(Command<? super Arg, ? super Result> command, Arg arg, Request<MongodConnection> request)
            throws MongoException, CommandNotSupportedException {

        try (MongodTransaction trans = request.getConnection().openTransaction(true)) {
            return trans.execute(command, arg, request);
        }
    }

    @Override
    public CommandsLibrary getCommandsLibrary() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public ReplyMessage query(Request request, QueryRequest build) throws MongoException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public ReplyMessage getMore(Request request, GetMoreMessage getMoreMessage) throws
            MongoException {

    }

    @Override
    public void killCursors(Request req, KillCursorsMessage killCursorsMessage) throws
            MongoException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void insert(Request req, InsertMessage insertMessage) throws MongoException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void update(Request req, UpdateMessage updateMessage) throws MongoException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void delete(Request req, DeleteMessage deleteMessage) throws MongoException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

}
