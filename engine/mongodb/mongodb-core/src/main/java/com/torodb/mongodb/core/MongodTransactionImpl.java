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
import com.eightkdata.mongowp.server.api.Request;
import com.google.common.base.Preconditions;
import com.torodb.core.transaction.RollbackException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 */
abstract class MongodTransactionImpl implements MongodTransaction {

  private static final Logger LOGGER = LogManager.getLogger(MongodTransactionImpl.class);

  private final MongodConnection connection;
  private Request currentRequest;
  private boolean closed = false;

  MongodTransactionImpl(MongodConnection connection) {
    this.connection = connection;
  }

  protected abstract <A, R> Status<R> executeProtected(Request req,
      Command<? super A, ? super R> command, A arg);

  @Override
  public MongodConnection getConnection() {
    return connection;
  }

  @Override
  public <A, R> Status<R> execute(Request req,
      Command<? super A, ? super R> command, A arg) throws RollbackException {
    Preconditions.checkState(currentRequest == null,
        "Another request is currently under execution. Request is " + currentRequest);
    this.currentRequest = req;
    try {
      Status<R> status = executeProtected(req, command, arg);
      return status;
    } finally {
      this.currentRequest = null;
    }
  }

  @Override
  public Request getCurrentRequest() {
    return currentRequest;
  }

  @Override
  public void rollback() {
    getTorodTransaction().rollback();
  }

  @Override
  public void close() {
    if (!closed) {
      closed = true;
      getTorodTransaction().close();
      connection.onTransactionClosed(this);
    }
  }

  @Override
  public boolean isClosed() {
    return closed;
  }

  @Override
  protected void finalize() throws Throwable {
    if (!closed) {
      LOGGER.warn(this.getClass() + " finalized without being closed");
      close();
    }
  }

}
