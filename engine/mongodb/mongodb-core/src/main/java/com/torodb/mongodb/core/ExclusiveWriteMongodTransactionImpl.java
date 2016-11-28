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

package com.torodb.mongodb.core;

import com.eightkdata.mongowp.Status;
import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.CommandsExecutor;
import com.eightkdata.mongowp.server.api.Request;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.transaction.RollbackException;
import com.torodb.torod.ExclusiveWriteTorodTransaction;

class ExclusiveWriteMongodTransactionImpl extends MongodTransactionImpl implements
    ExclusiveWriteMongodTransaction {

  private final ExclusiveWriteTorodTransaction torodTransaction;
  private final CommandsExecutor<? super ExclusiveWriteMongodTransactionImpl> commandsExecutor;

  public ExclusiveWriteMongodTransactionImpl(MongodConnection connection, boolean concurrent) {
    super(connection);
    this.torodTransaction = connection.getTorodConnection()
        .openExclusiveWriteTransaction(concurrent);
    this.commandsExecutor = connection.getServer().getCommandsExecutorClassifier()
        .getExclusiveWriteCommandsExecutor();
  }

  @Override
  public ExclusiveWriteTorodTransaction getTorodTransaction() {
    return torodTransaction;
  }

  @Override
  protected <A, R> Status<R> executeProtected(Request req,
      Command<? super A, ? super R> command, A arg) {
    return commandsExecutor.execute(req, command, arg, this);
  }

  @Override
  public void commit() throws RollbackException, UserException {
    torodTransaction.commit();
  }

}
