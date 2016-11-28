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

package com.torodb.stampede;

import com.torodb.core.backend.BackendConnection;
import com.torodb.core.backend.BackendService;
import com.torodb.core.backend.BackendTransaction;
import com.torodb.core.backend.MetaInfoKey;
import com.torodb.core.backend.WriteBackendTransaction;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.retrier.Retrier;
import com.torodb.core.retrier.RetrierAbortException;
import com.torodb.core.retrier.RetrierGiveUpException;
import com.torodb.core.transaction.RollbackException;
import com.torodb.kvdocument.types.BooleanType;
import com.torodb.kvdocument.values.KvBoolean;
import com.torodb.kvdocument.values.KvValue;
import com.torodb.mongodb.repl.ConsistencyHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Optional;

import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public class DefaultConsistencyHandler implements ConsistencyHandler {

  private static final Logger LOGGER =
      LogManager.getLogger(DefaultConsistencyHandler.class);
  private boolean consistent;
  private static final MetaInfoKey CONSISTENCY_KEY = () -> "repl.consistent";
  private final BackendService backendService;
  private final Retrier retrier;

  DefaultConsistencyHandler(BackendService backendService, Retrier retrier) {
    this.backendService = backendService;
    this.retrier = retrier;

    loadConsistent();
  }

  @Override
  public boolean isConsistent() {
    return consistent;
  }

  @Override
  public void setConsistent(boolean consistency) throws RetrierGiveUpException {
    this.consistent = consistency;
    flushConsistentState();
    LOGGER.info("Consistent state has been set to '" + consistent + "'");
  }

  private void loadConsistent() {
    try (BackendConnection conn = backendService.openConnection();
        BackendTransaction trans = conn.openReadOnlyTransaction()) {
      Optional<KvValue<?>> valueOpt = trans.readMetaInfo(CONSISTENCY_KEY);
      if (!valueOpt.isPresent()) {
        consistent = false;
        return;
      }
      KvValue<?> value = valueOpt.get();
      if (!value.getType().equals(BooleanType.INSTANCE)) {
        throw new IllegalStateException("Unexpected consistency value "
            + "found. Expected a boolean but " + valueOpt + " was "
            + "found");
      }
      consistent = ((KvBoolean) value).getPrimitiveValue();
    }
  }

  private void flushConsistentState() throws RollbackException, RetrierGiveUpException {
    try (BackendConnection conn = backendService.openConnection()) {
      retrier.retry(() -> flushConsistentState(conn));
    }
  }

  private Object flushConsistentState(BackendConnection conn) throws RetrierAbortException {
    try (WriteBackendTransaction trans = conn.openSharedWriteTransaction()) {

      trans.writeMetaInfo(CONSISTENCY_KEY, KvBoolean.from(consistent));
      trans.commit();
    } catch (UserException ex) {
      throw new RetrierAbortException(ex);
    }
    return null;
  }

}
