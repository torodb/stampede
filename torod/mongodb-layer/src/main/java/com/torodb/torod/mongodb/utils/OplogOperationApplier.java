
package com.torodb.torod.mongodb.utils;

import com.eightkdata.mongowp.server.api.oplog.DbOplogOperation;
import com.eightkdata.mongowp.server.api.oplog.OplogOperationVisitor;
import com.eightkdata.mongowp.server.api.oplog.UpdateOplogOperation;
import com.eightkdata.mongowp.server.api.oplog.DeleteOplogOperation;
import com.eightkdata.mongowp.server.api.oplog.DbCmdOplogOperation;
import com.eightkdata.mongowp.server.api.oplog.InsertOplogOperation;
import com.eightkdata.mongowp.server.api.oplog.OplogOperation;
import com.eightkdata.mongowp.server.api.oplog.NoopOplogOperation;
import com.eightkdata.mongowp.WriteConcern;
import com.eightkdata.mongowp.bson.BsonDocument;
import com.eightkdata.mongowp.client.core.MongoConnection;
import com.eightkdata.mongowp.exceptions.*;
import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.CommandsLibrary;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.admin.CreateIndexesCommand;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.admin.CreateIndexesCommand.CreateIndexesArgument;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.DeleteCommand;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.DeleteCommand.DeleteArgument;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.DeleteCommand.DeleteStatement;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.InsertCommand;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.InsertCommand.InsertArgument;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.InsertCommand.InsertResult;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.UpdateCommand;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.UpdateCommand.UpdateArgument;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.UpdateCommand.UpdateResult;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.UpdateCommand.UpdateStatement;
import com.torodb.torod.mongodb.impl.LocalMongoConnection;
import com.torodb.torod.mongodb.repl.OplogManager;
import com.torodb.torod.mongodb.repl.OplogManager.OplogManagerPersistException;
import java.util.Collections;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.LoggerFactory;

import static com.eightkdata.mongowp.bson.utils.DefaultBsonValues.*;

/**
 *
 */
@Singleton
public class OplogOperationApplier {

    private static final org.slf4j.Logger LOGGER
            = LoggerFactory.getLogger(OplogOperationApplier.class);
    private final CommandsLibrary library;
    private final Visitor visitor = new Visitor();
    private final DbCmdApplierFuction dbCmdApplierFunction = new DbCmdApplierFuction();

    @Inject
    public OplogOperationApplier(CommandsLibrary library) {
        this.library = library;
    }

    public boolean apply(
            OplogOperation op,
            LocalMongoConnection connection,
            OplogManager.WriteTransaction myOplogTrans,
            boolean updatesAsUpserts) throws MongoException, OplogManagerPersistException {
       boolean result = op.accept(visitor, null).apply(
               op,
               connection,
               myOplogTrans,
               updatesAsUpserts
       );
       if (result) {
           myOplogTrans.addOperation(op);
       }
       return result;
    }

    private class Visitor implements OplogOperationVisitor<OplogOperationApplierFunction, Void> {

        @Override
        public OplogOperationApplierFunction visit(DbCmdOplogOperation op, Void arg) {
            return dbCmdApplierFunction;
        }

        @Override
        public OplogOperationApplierFunction visit(DbOplogOperation op, Void arg) {
            return DbApplierFunction.INSTANCE;
        }

        @Override
        public OplogOperationApplierFunction visit(DeleteOplogOperation op, Void arg) {
            return DeleteApplierFuction.INSTANCE;
        }

        @Override
        public OplogOperationApplierFunction visit(InsertOplogOperation op, Void arg) {
            return InsertApplierFunction.INSTANCE;
        }

        @Override
        public OplogOperationApplierFunction visit(NoopOplogOperation op, Void arg) {
            return NoopApplierFunction.INSTANCE;
        }

        @Override
        public OplogOperationApplierFunction visit(UpdateOplogOperation op, Void arg) {
            return UpdateApplierFunction.INSTANCE;
        }
    }

    private static interface OplogOperationApplierFunction<E extends OplogOperation> {
        public boolean apply(
                E op,
                LocalMongoConnection connection,
                OplogManager.WriteTransaction myOplogTrans,
                boolean updatesAsUpserts) throws MongoException;
    }

    private static class InsertApplierFunction implements OplogOperationApplierFunction<InsertOplogOperation> {

        private static InsertApplierFunction INSTANCE = new InsertApplierFunction();
        
        @Override
        public boolean apply(
                InsertOplogOperation op,
                LocalMongoConnection connection,
                OplogManager.WriteTransaction myOplogTrans,
                boolean updatesAsUpserts)
                throws MongoException {
            BsonDocument docToInsert = op.getDocToInsert();
            if (NamespaceUtil.isIndexesMetaCollection(op.getCollection())) {
                insertIndex(docToInsert, op.getDatabase(), connection);
            } else {
                insertDocument(op, connection);
            }
            return true;
        }

        private void insertIndex(BsonDocument indexDoc, String database, MongoConnection connection)
                throws TypesMismatchException, NoSuchKeyException, BadValueException, MongoException {
            CreateIndexesCommand command = CreateIndexesCommand.INSTANCE;
            CreateIndexesArgument arg = command.unmarshallArg(indexDoc);

            connection.execute(command, database, true, arg);
        }

        private void insertDocument(InsertOplogOperation op, MongoConnection connection) throws MongoException {

            BsonDocument docToInsert = op.getDocToInsert();
            
            //Inserts must be executed as upserts to be idempotent
            BsonDocument query;
            if (!DefaultIdUtils.containsDefaultId(docToInsert)) {
                query = docToInsert;  //as we dont have $_id, we need the whole document to be sure selector is correct
            } else {
                query = newDocument(
                        DefaultIdUtils.DEFAULT_ID_KEY,
                        DefaultIdUtils.getDefaultId(docToInsert)
                );
            }
            while (true) {
                //TODO: Check that the operation is executed even if the execution is interrupted!
                //TODO: Use upsert to insert.
                UpdateResult updateResult = connection.execute(
                        UpdateCommand.INSTANCE,
                        op.getDatabase(),
                        true,
                        new UpdateArgument(
                                op.getCollection(),
                                Collections.singletonList(
                                        new UpdateStatement(query, docToInsert, false, false)
                                ),
                                true,
                                WriteConcern.fsync()
                        )
                );
                assert updateResult.getModifiedCounter() <= 1;
                if (updateResult.getModifiedCounter() == 0) {
                    InsertResult insertResult = connection.execute(
                            InsertCommand.INSTANCE,
                            op.getDatabase(),
                            true,
                            new InsertArgument.Builder(op.getCollection())
                            .addDocument(docToInsert)
                            .build()
                    );
                    assert insertResult.getN() == 1;
                }
                break;
            }
        }
    }

    private static class UpdateApplierFunction implements OplogOperationApplierFunction<UpdateOplogOperation> {

        private static UpdateApplierFunction INSTANCE = new UpdateApplierFunction();
        
        @Override
        public boolean apply(
                UpdateOplogOperation op,
                LocalMongoConnection connection,
                OplogManager.WriteTransaction myOplogTrans,
                boolean updatesAsUpserts)
                throws MongoException {

            boolean upsert = op.isUpsert() || updatesAsUpserts;

            while (true) {
                //TODO: Check that the operation is executed even if the execution is interrupted!
                UpdateResult result = connection.execute(
                        UpdateCommand.INSTANCE,
                        op.getDatabase(),
                        true,
                        new UpdateArgument(
                                op.getCollection(),
                                Collections.singletonList(
                                        new UpdateStatement(op.getFilter(), op.getModification(), upsert, true)
                                ),
                                true,
                                WriteConcern.fsync()
                        )
                );

                if (!result.isOk()) {
                    //TODO: throw a more specific exception
                    throw new UnknownErrorException("Error while applying a update oplog operation");
                }

                if (result.getModifiedCounter() != 0) {
                    return true;
                }

                if (!result.getUpserts().isEmpty()) {
                    LOGGER.warn("Replication couldn't find doc for op " + op);
                    return false;
                }
                return true;
            }
        }

    }

    private static class DeleteApplierFuction implements OplogOperationApplierFunction<DeleteOplogOperation> {

        private static DeleteApplierFuction INSTANCE = new DeleteApplierFuction();

        @Override
        public boolean apply(
                DeleteOplogOperation op,
                LocalMongoConnection connection,
                OplogManager.WriteTransaction myOplogTrans,
                boolean updatesAsUpserts)
                throws MongoException {
            //TODO: Check that the operation is executed even if the execution is interrupted!
            connection.execute(
                    DeleteCommand.INSTANCE,
                    op.getDatabase(),
                    true,
                    new DeleteArgument(
                            op.getCollection(),
                            Collections.singletonList(
                                    new DeleteStatement(op.getFilter(), op.isJustOne())
                            ),
                            true,
                            WriteConcern.fsync()
                    )
            );
            return true;
        }

    }

    private class DbCmdApplierFuction implements OplogOperationApplierFunction<DbCmdOplogOperation> {

        @Override
        public boolean apply(
                DbCmdOplogOperation op,
                LocalMongoConnection connection,
                OplogManager.WriteTransaction myOplogTrans,
                boolean updatesAsUpserts)
                throws MongoException {

            Command command = library.find(op.getRequest());
            if (command == null) {
                BsonDocument document = op.getRequest();
                if (document.isEmpty()) {
                    throw new CommandNotFoundException("Empty document query");
                }
                String firstKey = document.getFirstEntry().getKey();
                throw new CommandNotFoundException(firstKey);
            }
            Object arg = command.unmarshallArg(op.getRequest());

            connection.execute(command, op.getDatabase(), true, arg);
            
            return true;
        }

    }

    private static class NoopApplierFunction implements OplogOperationApplierFunction<NoopOplogOperation> {

        private static final NoopApplierFunction INSTANCE = new NoopApplierFunction();

        @Override
        public boolean apply(
                NoopOplogOperation op,
                LocalMongoConnection connection,
                OplogManager.WriteTransaction myOplogTrans,
                boolean updatesAsUpserts)
                throws MongoException {
            LOGGER.debug("Ignoring a noop operation");
            return true;
        }

    }

    private static class DbApplierFunction implements OplogOperationApplierFunction<DbOplogOperation> {

        private static final DbApplierFunction INSTANCE = new DbApplierFunction();

        @Override
        public boolean apply(
                DbOplogOperation op,
                LocalMongoConnection connection,
                OplogManager.WriteTransaction myOplogTrans,
                boolean updatesAsUpserts)
                throws MongoException {
            LOGGER.debug("Ignoring a db operation");
            return true;
        }
    }

}
