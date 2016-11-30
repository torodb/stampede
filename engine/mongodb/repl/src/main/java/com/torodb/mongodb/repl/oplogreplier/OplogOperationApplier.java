/*
 * ToroDB
 * Copyright © 2014 8Kdata Technology (www.8kdata.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.torodb.mongodb.repl.oplogreplier;

import static com.eightkdata.mongowp.bson.utils.DefaultBsonValues.newDocument;

import com.eightkdata.mongowp.ErrorCode;
import com.eightkdata.mongowp.Status;
import com.eightkdata.mongowp.WriteConcern;
import com.eightkdata.mongowp.bson.BsonDocument;
import com.eightkdata.mongowp.exceptions.CommandNotFoundException;
import com.eightkdata.mongowp.exceptions.MongoException;
import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.CommandsLibrary.LibraryEntry;
import com.eightkdata.mongowp.server.api.Request;
import com.eightkdata.mongowp.server.api.oplog.DbCmdOplogOperation;
import com.eightkdata.mongowp.server.api.oplog.DbOplogOperation;
import com.eightkdata.mongowp.server.api.oplog.DeleteOplogOperation;
import com.eightkdata.mongowp.server.api.oplog.InsertOplogOperation;
import com.eightkdata.mongowp.server.api.oplog.NoopOplogOperation;
import com.eightkdata.mongowp.server.api.oplog.OplogOperation;
import com.eightkdata.mongowp.server.api.oplog.OplogOperationVisitor;
import com.eightkdata.mongowp.server.api.oplog.UpdateOplogOperation;
import com.torodb.mongodb.commands.pojos.index.IndexOptions;
import com.torodb.mongodb.commands.signatures.admin.CreateIndexesCommand;
import com.torodb.mongodb.commands.signatures.admin.CreateIndexesCommand.CreateIndexesArgument;
import com.torodb.mongodb.commands.signatures.general.DeleteCommand;
import com.torodb.mongodb.commands.signatures.general.DeleteCommand.DeleteArgument;
import com.torodb.mongodb.commands.signatures.general.DeleteCommand.DeleteStatement;
import com.torodb.mongodb.commands.signatures.general.InsertCommand;
import com.torodb.mongodb.commands.signatures.general.UpdateCommand;
import com.torodb.mongodb.commands.signatures.general.UpdateCommand.UpdateArgument;
import com.torodb.mongodb.commands.signatures.general.UpdateCommand.UpdateResult;
import com.torodb.mongodb.commands.signatures.general.UpdateCommand.UpdateStatement;
import com.torodb.mongodb.core.ExclusiveWriteMongodTransaction;
import com.torodb.mongodb.repl.OplogManager;
import com.torodb.mongodb.repl.commands.ReplCommandsExecutor;
import com.torodb.mongodb.repl.commands.ReplCommandsLibrary;
import com.torodb.mongodb.utils.DefaultIdUtils;
import com.torodb.mongodb.utils.NamespaceUtil;
import com.torodb.torod.ExclusiveWriteTorodTransaction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.Collections;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

@ThreadSafe
public class OplogOperationApplier {

  private static final Logger LOGGER = LogManager.getLogger(OplogOperationApplier.class);
  private final Visitor visitor = new Visitor();
  private final ReplCommandsLibrary library;
  private final ReplCommandsExecutor executor;

  @Inject
  public OplogOperationApplier(ReplCommandsLibrary library, ReplCommandsExecutor executor) {
    this.library = library;
    this.executor = executor;
  }

  /**
   * Applies the given {@link OplogOperation} on the database.
   *
   * This method <b>DO NOT</b> modify the {@link OplogManager} state.
   *
   * @param op
   * @param transaction
   * @param applierContext
   * @throws com.torodb.mongodb.repl.oplogreplier.OplogOperationApplier.OplogApplyingException
   */
  @SuppressWarnings("unchecked")
  public void apply(OplogOperation op,
      ExclusiveWriteMongodTransaction transaction,
      ApplierContext applierContext) throws OplogApplyingException {
    op.accept(visitor, null).apply(
        op,
        transaction,
        applierContext
    );
  }

  private <A, R> Status<R> executeReplCommand(String db, Command<? super A, ? super R> command,
      A arg, ExclusiveWriteTorodTransaction trans) {
    Request req = new Request(db, null, true, null);

    Status<R> result = executor.execute(req, command, arg, trans);

    return result;
  }

  private <A, R> Status<R> executeTorodCommand(String db, Command<? super A, ? super R> command,
      A arg, ExclusiveWriteMongodTransaction trans) throws MongoException {
    Request req = new Request(db, null, true, null);

    Status<R> result = trans.execute(req, command, arg);

    return result;
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
      ExclusiveWriteMongodTransaction trans,
      ApplierContext applierContext) throws OplogApplyingException {
    BsonDocument docToInsert = op.getDocToInsert();
    if (NamespaceUtil.isIndexesMetaCollection(op.getCollection())) {
      insertIndex(docToInsert, op.getDatabase(), trans);
    } else {
      try {
        insertDocument(op, trans);
      } catch (MongoException ex) {
        throw new OplogApplyingException(ex);
      }
    }
  }

  private void insertIndex(BsonDocument indexDoc, String database,
      ExclusiveWriteMongodTransaction trans) throws OplogApplyingException {
    try {
      CreateIndexesCommand command = CreateIndexesCommand.INSTANCE;
      IndexOptions indexOptions = IndexOptions.unmarshall(indexDoc);

      CreateIndexesArgument arg = new CreateIndexesArgument(
          indexOptions.getCollection(), Arrays.asList(
          new IndexOptions[]{indexOptions}));

      Status executionResult = executeReplCommand(database, command, arg,
          trans.getTorodTransaction());
      if (!executionResult.isOk()) {
        throw new OplogApplyingException(new MongoException(executionResult));
      }
    } catch (MongoException ex) {
      throw new OplogApplyingException(ex);
    }
  }

  private void insertDocument(InsertOplogOperation op,
      ExclusiveWriteMongodTransaction trans) throws MongoException {

    BsonDocument docToInsert = op.getDocToInsert();

    //TODO: Inserts must be executed as upserts to be idempotent
    //TODO: This implementation works iff this connection is the only one that is writing
    BsonDocument query;
    if (!DefaultIdUtils.containsDefaultId(docToInsert)) {
      //as we dont have _id, we need the whole document to be sure selector is correct
      query = docToInsert;
    } else {
      query = newDocument(
          DefaultIdUtils.DEFAULT_ID_KEY,
          DefaultIdUtils.getDefaultId(docToInsert)
      );
    }
    while (true) {
      executeTorodCommand(
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
      executeTorodCommand(
          op.getDatabase(),
          InsertCommand.INSTANCE,
          new InsertCommand.InsertArgument(op.getCollection(), Collections
              .singletonList(docToInsert), WriteConcern.fsync(), true, null),
          trans);
      break;
    }
  }

  private void applyUpdate(
      UpdateOplogOperation op,
      ExclusiveWriteMongodTransaction trans,
      ApplierContext applierContext) throws OplogApplyingException {

    boolean upsert = op.isUpsert() || applierContext.treatUpdateAsUpsert();

    Status<UpdateResult> status;
    try {
      status = executeTorodCommand(
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

    if (!status.isOk()) {
      //TODO: improve error code
      throw new OplogApplyingException(new MongoException(status));
    }
    UpdateResult updateResult = status.getResult();
    assert updateResult != null;
    if (!updateResult.isOk()) {
      throw new OplogApplyingException(new MongoException(updateResult.getErrorMessage(),
          ErrorCode.UNKNOWN_ERROR));
    }

    if (!upsert && updateResult.getModifiedCounter() != 0) {
      LOGGER.info("Oplog update operation with optime {} and hash {} did not find the doc to "
          + "modify. Filter is {}", op.getOpTime(), op.getHash(), op.getFilter());
    }

    if (upsert && !updateResult.getUpserts().isEmpty()) {
      LOGGER.warn("Replication couldn't find doc for op " + op);
    }
  }

  private void applyDelete(
      DeleteOplogOperation op,
      ExclusiveWriteMongodTransaction trans,
      ApplierContext applierContext) throws OplogApplyingException {
    try {
      //TODO: Check that the operation is executed even if the execution is interrupted!
      Status<Long> status = executeTorodCommand(
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
      if (!status.isOk()) {
        throw new OplogApplyingException(new MongoException(status));
      }
      if (status.getResult() == 0 && applierContext.treatUpdateAsUpsert()) {
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
      ExclusiveWriteMongodTransaction trans,
      ApplierContext applierContext) throws OplogApplyingException {

    LibraryEntry librayEntry = library.find(op.getRequest());

    if (librayEntry == null) {
      throw new OplogApplyingException(new CommandNotFoundException(
          op.getRequest().isEmpty() ? "?" : op.getRequest().getFirstEntry().getKey()));
    }

    Command command = librayEntry.getCommand();
    if (command == null) {
      BsonDocument document = op.getRequest();
      if (document.isEmpty()) {
        throw new OplogApplyingException(new CommandNotFoundException(
            "Empty document query"));
      }
      String firstKey = document.getFirstEntry().getKey();
      throw new OplogApplyingException(new CommandNotFoundException(firstKey));
    }
    Object arg;
    try {
      arg = command.unmarshallArg(op.getRequest(), librayEntry.getAlias());
    } catch (MongoException ex) {
      throw new OplogApplyingException(ex);
    }

    Status executionResult = executeReplCommand(op.getDatabase(), command,
        arg, trans.getTorodTransaction());
    if (!executionResult.isOk()) {
      throw new OplogApplyingException(new MongoException(executionResult));
    }
  }

  @FunctionalInterface
  private static interface OplogOperationApplierFunction<E extends OplogOperation> {

    public void apply(
        E op,
        ExclusiveWriteMongodTransaction transaction,
        ApplierContext applierContext) throws OplogApplyingException;
  }
}
