
package com.torodb.mongodb.repl;

import com.eightkdata.mongowp.ErrorCode;
import com.eightkdata.mongowp.Status;
import com.eightkdata.mongowp.WriteConcern;
import com.eightkdata.mongowp.bson.BsonDocument;
import com.eightkdata.mongowp.exceptions.ConflictingOperationInProgressException;
import com.eightkdata.mongowp.exceptions.MongoException;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.admin.CreateIndexesCommand;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.admin.CreateIndexesCommand.CreateIndexesArgument;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.admin.CreateIndexesCommand.CreateIndexesResult;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.DeleteCommand;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.DeleteCommand.DeleteArgument;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.DeleteCommand.DeleteStatement;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.InsertCommand;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.UpdateCommand;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.UpdateCommand.UpdateArgument;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.UpdateCommand.UpdateResult;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.UpdateCommand.UpdateStatement;
import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.Request;
import com.eightkdata.mongowp.server.api.oplog.*;
import com.torodb.mongodb.commands.TorodbCommandsLibrary;
import com.torodb.mongodb.core.WriteMongodTransaction;
import com.torodb.mongodb.repl.OplogManager.OplogManagerPersistException;
import com.torodb.mongodb.utils.DefaultIdUtils;
import com.torodb.mongodb.utils.NamespaceUtil;
import java.util.Collections;
import java.util.concurrent.Callable;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static com.eightkdata.mongowp.bson.utils.DefaultBsonValues.newDocument;

/**
 *
 */
@Singleton
public class OplogOperationApplier {

    private static final Logger LOGGER = LogManager.getLogger(OplogOperationApplier.class);
    private final TorodbCommandsLibrary library;
    private final Visitor visitor = new Visitor();

    @Inject
    public OplogOperationApplier(TorodbCommandsLibrary library) {
        this.library = library;
    }

    public Status<?> apply(
            OplogOperation op,
            WriteMongodTransaction transaction,
            OplogManager.WriteTransaction myOplogTrans,
            boolean updatesAsUpserts) throws OplogManagerPersistException {
       @SuppressWarnings("unchecked")
       Status<?> result = op.accept(visitor, null).apply(
               op,
               transaction,
               myOplogTrans,
               updatesAsUpserts
       );
       if (result.isOK()) {
           myOplogTrans.addOperation(op);
       }
       return result;
    }

    private <Arg, Result> Result executeCommand(String db, Command<? super Arg, ? super Result> command, Arg arg, WriteMongodTransaction trans) throws MongoException {
        Callable<Status<Result>> callable;

        Request req = new Request(db, null, true, null);
        
        
        Status<Result> result = trans.execute(req, command, arg);
        
        if (result == null) {
            throw new ConflictingOperationInProgressException("It was impossible to execute "
                    + command.getCommandName() + " after several attempts");
        }
        return result.getResult();
    }

    private class Visitor implements OplogOperationVisitor<OplogOperationApplierFunction, Void> {

        @Override
        public OplogOperationApplierFunction visit(DbCmdOplogOperation op, Void arg) {
            return (operation, trans, oplogTrans, updatesAsUpserts) ->
                    applyCmd((DbCmdOplogOperation) operation, trans, oplogTrans, updatesAsUpserts);
        }

        @Override
        public OplogOperationApplierFunction visit(DbOplogOperation op, Void arg) {
            return (operation, con, oplogTrans, updatesAsUpserts) -> {
                LOGGER.debug("Ignoring a db operation");
                return Status.ok();
            };
        }

        @Override
        public OplogOperationApplierFunction visit(DeleteOplogOperation op, Void arg) {
            return (operation, con, oplogTrans, updatesAsUpserts) ->
                    applyDelete((DeleteOplogOperation) operation, con, oplogTrans, updatesAsUpserts);
        }

        @Override
        public OplogOperationApplierFunction visit(InsertOplogOperation op, Void arg) {
            return (operation, con, oplogTrans, updatesAsUpserts) ->
                    applyInsert((InsertOplogOperation) operation, con, oplogTrans, updatesAsUpserts);
        }

        @Override
        public OplogOperationApplierFunction visit(NoopOplogOperation op, Void arg) {
            return (operation, con, oplogTrans, updatesAsUpserts) -> {
                LOGGER.debug("Ignoring a noop operation");
                return Status.ok();
            };
        }

        @Override
        public OplogOperationApplierFunction visit(UpdateOplogOperation op, Void arg) {
            return (operation, con, oplogTrans, updatesAsUpserts) ->
                    applyUpdate((UpdateOplogOperation) operation, con, oplogTrans, updatesAsUpserts);
        }
    }

    public Status<?> applyInsert(
            InsertOplogOperation op,
            WriteMongodTransaction trans,
            OplogManager.WriteTransaction myOplogTrans,
            boolean updatesAsUpserts) {
        BsonDocument docToInsert = op.getDocToInsert();
        try {
            if (NamespaceUtil.isIndexesMetaCollection(op.getCollection())) {
                insertIndex(docToInsert, op.getDatabase(), trans);
            } else {
                insertDocument(op, trans);
            }
            return Status.ok();
        } catch (MongoException ex) {
            return Status.from(ex);
        }
    }

    private Status<CreateIndexesResult> insertIndex(BsonDocument indexDoc, String database, WriteMongodTransaction trans) {
        try {
            CreateIndexesCommand command = CreateIndexesCommand.INSTANCE;
            CreateIndexesArgument arg = command.unmarshallArg(indexDoc);

            CreateIndexesResult result = executeCommand(database, command, arg, trans);
            return Status.ok(result);
        } catch (MongoException ex) {
            return Status.from(ex);
        }
    }

    private void insertDocument(InsertOplogOperation op, WriteMongodTransaction trans) throws
            MongoException {

        BsonDocument docToInsert = op.getDocToInsert();

        //TODO: Inserts must be executed as upserts to be idempotent
        //TODO: This implementation works iff this connection is the only one that is writing
        BsonDocument query;
        if (!DefaultIdUtils.containsDefaultId(docToInsert)) {
            query = docToInsert;  //as we dont have _id, we need the whole document to be sure selector is correct
        } else {
            query = newDocument(
                    DefaultIdUtils.DEFAULT_ID_KEY,
                    DefaultIdUtils.getDefaultId(docToInsert)
            );
        }
        while (true) {
            executeCommand(
                    op.getDatabase(),
                    DeleteCommand.INSTANCE,
                    new DeleteCommand.DeleteArgument(
                            op.getCollection(),
                            Collections.singletonList(
                                    new DeleteCommand.DeleteStatement(query, true)
                            ),
                            true,
                            null
                    ),
                    trans);
            executeCommand(
                    op.getDatabase(),
                    InsertCommand.INSTANCE,
                    new InsertCommand.InsertArgument(op.getCollection(), Collections.singletonList(docToInsert), WriteConcern.fsync(), true, null),
                    trans);
            break;
        }
    }

    private Status<UpdateResult> applyUpdate(
            UpdateOplogOperation op,
            WriteMongodTransaction trans,
            OplogManager.WriteTransaction myOplogTrans,
            boolean updatesAsUpserts) {

        boolean upsert = op.isUpsert() || updatesAsUpserts;

        UpdateResult result;
        try {
            result = executeCommand(
                    op.getDatabase(),
                    UpdateCommand.INSTANCE,
                    new UpdateArgument(
                            op.getCollection(),
                            Collections.singletonList(
                                    new UpdateStatement(op.getFilter(), op.getModification(), upsert, true)
                            ),
                            true,
                            WriteConcern.fsync()
                    ),
                    trans
            );
        } catch (MongoException ex) {
            return Status.from(ex);
        }

        if (!result.isOk()) {
            //TODO: throw a more specific exception
            return Status.from(ErrorCode.UNKNOWN_ERROR, "Error while applying a update oplog operation: " + result.getErrorMessage());
        }

        if (result.getModifiedCounter() != 0) {
            return Status.ok(result);
        }

        if (!result.getUpserts().isEmpty()) {
            LOGGER.warn("Replication couldn't find doc for op " + op);
        }
        return Status.ok(result);
    }

    private Status<Long> applyDelete(
            DeleteOplogOperation op,
            WriteMongodTransaction trans,
            OplogManager.WriteTransaction myOplogTrans,
            boolean updatesAsUpserts) {
        try {
            //TODO: Check that the operation is executed even if the execution is interrupted!
            Long result = executeCommand(
                    op.getDatabase(),
                    DeleteCommand.INSTANCE,
                    new DeleteArgument(
                            op.getCollection(),
                            Collections.singletonList(
                                    new DeleteStatement(op.getFilter(), op.isJustOne())
                            ),
                            true,
                            WriteConcern.fsync()
                    ),
                    trans
            );
            return Status.ok(result);
        } catch (MongoException ex) {
            return Status.from(ex);
        }
    }

    @SuppressWarnings("unchecked")
    private Status<?> applyCmd(
            DbCmdOplogOperation op,
            WriteMongodTransaction trans,
            OplogManager.WriteTransaction myOplogTrans,
            boolean updatesAsUpserts) {

        Command command = library.find(op.getRequest());
        if (command == null) {
            BsonDocument document = op.getRequest();
            if (document.isEmpty()) {
                return Status.from(ErrorCode.COMMAND_NOT_FOUND, "Empty document query");
            }
            String firstKey = document.getFirstEntry().getKey();
            return Status.withDefaultMsg(ErrorCode.COMMAND_NOT_FOUND, firstKey);
        }
        Object arg;
        try {
            arg = command.unmarshallArg(op.getRequest());
            Object result = executeCommand(op.getDatabase(), command, arg, trans);
            return Status.ok(result);
        } catch (MongoException ex) {
            return Status.from(ex);
        }
    }

    @FunctionalInterface
    private static interface OplogOperationApplierFunction<E extends OplogOperation> {
        public Status<?> apply(
                E op,
                WriteMongodTransaction transaction,
                OplogManager.WriteTransaction myOplogTrans,
                boolean updatesAsUpserts);
    }
}
