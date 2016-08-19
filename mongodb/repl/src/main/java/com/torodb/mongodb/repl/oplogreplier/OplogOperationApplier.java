
package com.torodb.mongodb.repl.oplogreplier;

import com.eightkdata.mongowp.ErrorCode;
import com.eightkdata.mongowp.Status;
import com.eightkdata.mongowp.WriteConcern;
import com.eightkdata.mongowp.bson.BsonDocument;
import com.eightkdata.mongowp.exceptions.CommandNotFoundException;
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
import com.torodb.mongodb.repl.OplogManager;
import com.torodb.mongodb.utils.DefaultIdUtils;
import com.torodb.mongodb.utils.NamespaceUtil;
import java.util.Collections;
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

    /**
     * Applies the given {@link OplogOperation} on the database.
     *
     * This method <b>DO NOT</b> modify the {@link OplogManager} state.
     * @param op
     * @param transaction
     * @param applierContext
     * @throws com.torodb.mongodb.repl.oplogreplier.OplogOperationApplier.OplogApplyingException
     */
    @SuppressWarnings("unchecked")
    public void apply(OplogOperation op,
            WriteMongodTransaction transaction,
            ApplierContext applierContext) throws OplogApplyingException {
        op.accept(visitor, null).apply(
                op,
                transaction,
                applierContext
        );
    }

    private <Arg, Result> Result executeCommand(String db, Command<? super Arg, ? super Result> command, Arg arg, WriteMongodTransaction trans) throws MongoException {
        Request req = new Request(db, null, true, null);
        
        
        Status<Result> result = trans.execute(req, command, arg);
        
        if (result == null) {
            throw new ConflictingOperationInProgressException("It was impossible to execute "
                    + command.getCommandName() + " after several attempts");
        }
        return result.getResult();
    }

    public static class OplogApplyingException extends Exception {
        private static final long serialVersionUID = 660910523948847788L;

        public OplogApplyingException(MongoException cause) {
            super(cause);
        }

    }

    @SuppressWarnings("rawtypes")
    private class Visitor implements OplogOperationVisitor<OplogOperationApplierFunction, Void> {

        @Override
        public OplogOperationApplierFunction visit(DbCmdOplogOperation op, Void arg) {
            return (operation, trans, applierContext) ->
                    applyCmd((DbCmdOplogOperation) operation, trans, applierContext);
        }

        @Override
        public OplogOperationApplierFunction visit(DbOplogOperation op, Void arg) {
            return (operation, con, applierContext) -> {
                LOGGER.debug("Ignoring a db operation");
            };
        }

        @Override
        public OplogOperationApplierFunction visit(DeleteOplogOperation op, Void arg) {
            return (operation, con, applierContext) ->
                    applyDelete((DeleteOplogOperation) operation, con, applierContext);
        }

        @Override
        public OplogOperationApplierFunction visit(InsertOplogOperation op, Void arg) {
            return (operation, con, applierContext) ->
                    applyInsert((InsertOplogOperation) operation, con, applierContext);
        }

        @Override
        public OplogOperationApplierFunction visit(NoopOplogOperation op, Void arg) {
            return (operation, con, applierContext) -> {
                LOGGER.debug("Ignoring a noop operation");
            };
        }

        @Override
        public OplogOperationApplierFunction visit(UpdateOplogOperation op, Void arg) {
            return (operation, con, applierContext) ->
                    applyUpdate((UpdateOplogOperation) operation, con, applierContext);
        }
    }

    private void applyInsert(
            InsertOplogOperation op,
            WriteMongodTransaction trans,
            ApplierContext applierContext) throws OplogApplyingException {
        BsonDocument docToInsert = op.getDocToInsert();
        try {
            if (NamespaceUtil.isIndexesMetaCollection(op.getCollection())) {
                insertIndex(docToInsert, op.getDatabase(), trans);
            } else {
                insertDocument(op, trans);
            }
        } catch (MongoException ex) {
            throw new OplogApplyingException(ex);
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

    private void applyUpdate(
            UpdateOplogOperation op,
            WriteMongodTransaction trans,
            ApplierContext applierContext) throws OplogApplyingException {

        boolean upsert = op.isUpsert() || applierContext.treatUpdateAsUpsert();

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
            throw new OplogApplyingException(ex);
        }

        if (!result.isOk()) {
            //TODO: improve error code
            throw new OplogApplyingException(new MongoException(result.getErrorMessage(), ErrorCode.UNKNOWN_ERROR));
        }

        if (!upsert && result.getModifiedCounter() != 0) {
            LOGGER.info("Oplog update operation with optime {} and hash {} did not find the doc to "
                    + "modify. Filter is {}", op.getOpTime(), op.getHash(), op.getFilter());
        }

        if (upsert && !result.getUpserts().isEmpty()) {
            LOGGER.warn("Replication couldn't find doc for op " + op);
        }
    }

    private void applyDelete(
            DeleteOplogOperation op,
            WriteMongodTransaction trans,
            ApplierContext applierContext) throws OplogApplyingException {
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
            if (result == 0 && applierContext.treatUpdateAsUpsert()) {
                LOGGER.info("Oplog delete operation with optime {} and hash {} did not find the "
                        + "doc to delete. Filter is {}", op.getOpTime(), op.getHash(), op.getFilter());
            }
        } catch (MongoException ex) {
            throw new OplogApplyingException(ex);
        }
    }

    @SuppressWarnings("unchecked")
    private void applyCmd(
            DbCmdOplogOperation op,
            WriteMongodTransaction trans,
            ApplierContext applierContext) throws OplogApplyingException {

        Command command = library.find(op.getRequest());
        if (command == null) {
            BsonDocument document = op.getRequest();
            if (document.isEmpty()) {
                throw new OplogApplyingException(new CommandNotFoundException("Empty document query"));
            }
            String firstKey = document.getFirstEntry().getKey();
            throw new OplogApplyingException(new CommandNotFoundException(firstKey));
        }
        Object arg;
        try {
            arg = command.unmarshallArg(op.getRequest());
            executeCommand(op.getDatabase(), command, arg, trans);
        } catch (MongoException ex) {
            throw new OplogApplyingException(ex);
        }
    }

    @FunctionalInterface
    private static interface OplogOperationApplierFunction<E extends OplogOperation> {
        public void apply(
                E op,
                WriteMongodTransaction transaction,
                ApplierContext applierContext) throws OplogApplyingException;
    }
}
