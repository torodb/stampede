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

package com.torodb.torod.impl.memory;

import com.google.common.base.Preconditions;
import com.torodb.torod.ExclusiveWriteTorodTransaction;
import com.torodb.torod.ReadOnlyTorodTransaction;
import com.torodb.torod.TorodConnection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 */
public class MemoryTorodConnection implements TorodConnection {

  private static final Logger LOGGER = LogManager.getLogger(MemoryTorodConnection.class);
  private final MemoryTorodServer server;
  private final int connectionId;
  private boolean closed = false;
  private MemoryTorodTransaction currentTransaction = null;

  MemoryTorodConnection(MemoryTorodServer server, int connectionId) {
    this.server = server;
    this.connectionId = connectionId;
  }

  @Override
  public ReadOnlyTorodTransaction openReadOnlyTransaction() {
    Preconditions.checkState(!closed, "This connection is closed");
    Preconditions.checkState(currentTransaction == null,
        "Another transaction is currently under execution. Transaction is " + currentTransaction);

    MemoryReadOnlyTorodTransaction result = new MemoryReadOnlyTorodTransaction(this);
    currentTransaction = result;
    return result;
  }

  @Override
  public ExclusiveWriteTorodTransaction openWriteTransaction(boolean concurrent) {
    Preconditions.checkState(!closed, "This connection is closed");
    Preconditions.checkState(currentTransaction == null,
        "Another transaction is currently under execution. Transaction is " + currentTransaction);

    MemoryWriteTorodTransaction result = new MemoryWriteTorodTransaction(this);
    this.currentTransaction = result;
    return result;
  }

  @Override
  public ExclusiveWriteTorodTransaction openExclusiveWriteTransaction(boolean concurrent) {
    return openWriteTransaction(concurrent);
  }

  @Override
  public int getConnectionId() {
    return connectionId;
  }

  @Override
  public MemoryTorodServer getServer() {
    return server;
  }

  @Override
  public void close() {
    if (!closed) {
      closed = true;
      if (currentTransaction != null) {
        currentTransaction.close();
      }
      server.onConnectionClosed(this);
    }
  }

  void onTransactionClosed(MemoryTorodTransaction transaction) {
    if (currentTransaction == null) {
      LOGGER.debug(
          "Recived an on transaction close notification, but there is no current transaction");
      return;
    }
    if (currentTransaction != transaction) {
      LOGGER.debug(
          "Recived an on transaction close notification, but the recived transaction is not the "
              + "same as the current one");
      return;
    }
    currentTransaction = null;
  }

}
