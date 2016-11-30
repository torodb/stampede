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

package com.torodb.backend;

import com.google.common.base.Preconditions;
import com.torodb.backend.meta.SchemaUpdater;
import com.torodb.core.backend.BackendConnection;
import com.torodb.core.backend.BackendTransaction;
import com.torodb.core.backend.ExclusiveWriteBackendTransaction;
import com.torodb.core.backend.ReadOnlyBackendTransaction;
import com.torodb.core.backend.SharedWriteBackendTransaction;
import com.torodb.core.d2r.IdentifierFactory;
import com.torodb.core.d2r.ReservedIdGenerator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 */
public class BackendConnectionImpl implements BackendConnection {

  private static final Logger LOGGER = LogManager.getLogger(BackendConnectionImpl.class);
  private final BackendServiceImpl backend;
  private final SqlInterface sqlInterface;
  private boolean closed = false;
  private final IdentifierFactory identifierFactory;
  private final ReservedIdGenerator ridGenerator;
  private BackendTransaction currentTransaction;

  public BackendConnectionImpl(BackendServiceImpl backend,
      SqlInterface sqlInterface, ReservedIdGenerator ridGenerator,
      IdentifierFactory identifierFactory) {
    this.backend = backend;
    this.sqlInterface = sqlInterface;
    this.identifierFactory = identifierFactory;
    this.ridGenerator = ridGenerator;
  }

  @Override
  public ReadOnlyBackendTransaction openReadOnlyTransaction() {
    Preconditions.checkState(!closed, "This connection is closed");
    Preconditions.checkState(currentTransaction == null,
        "Another transaction is currently under execution. Transaction is " + currentTransaction);

    ReadOnlyBackendTransactionImpl transaction = new ReadOnlyBackendTransactionImpl(sqlInterface,
        this);
    currentTransaction = transaction;

    return transaction;
  }

  @Override
  public SharedWriteBackendTransaction openSharedWriteTransaction() {
    Preconditions.checkState(!closed, "This connection is closed");
    Preconditions.checkState(currentTransaction == null,
        "Another transaction is currently under execution. Transaction is " + currentTransaction);

    SharedWriteBackendTransactionImpl transaction = new SharedWriteBackendTransactionImpl(
        sqlInterface, this, identifierFactory);
    currentTransaction = transaction;

    return transaction;
  }

  @Override
  public ExclusiveWriteBackendTransaction openExclusiveWriteTransaction() {
    Preconditions.checkState(!closed, "This connection is closed");
    Preconditions.checkState(currentTransaction == null,
        "Another transaction is currently under execution. Transaction is " + currentTransaction);

    ExclusiveWriteBackendTransactionImpl transaction = new ExclusiveWriteBackendTransactionImpl(
        sqlInterface, this, identifierFactory, ridGenerator);
    currentTransaction = transaction;

    return transaction;
  }

  KvMetainfoHandler getMetaInfoHandler() {
    return backend.getMetaInfoHandler();
  }

  SchemaUpdater getSchemaUpdater() {
    return backend.getSchemaUpdater();
  }

  @Override
  public void close() {
    if (!closed) {
      closed = true;
      if (currentTransaction != null) {
        currentTransaction.close();
      }
      assert currentTransaction == null;
      backend.onConnectionClosed(this);
    }
  }

  void onTransactionClosed(BackendTransaction transaction) {
    if (currentTransaction == null) {
      LOGGER.debug(
          "Recived an on transaction close notification, but there is no current transaction");
      return;
    }
    if (currentTransaction != transaction) {
      LOGGER.debug("Recived an on transaction close notification, but the recived transaction is "
          + "not the same as the current one");
      return;
    }
    currentTransaction = null;
  }

}
