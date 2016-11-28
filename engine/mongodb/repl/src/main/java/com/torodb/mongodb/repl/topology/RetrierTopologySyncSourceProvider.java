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

package com.torodb.mongodb.repl.topology;

import com.eightkdata.mongowp.OpTime;
import com.google.common.net.HostAndPort;
import com.torodb.core.retrier.Retrier;
import com.torodb.core.retrier.Retrier.Hint;
import com.torodb.core.retrier.RetrierGiveUpException;
import com.torodb.mongodb.repl.SyncSourceProvider;
import com.torodb.mongodb.repl.exceptions.NoSyncSourceFoundException;
import org.jooq.lambda.UncheckedException;

import java.util.Optional;
import java.util.concurrent.Callable;

import javax.inject.Inject;
import javax.inject.Singleton;

/**
 * A {@link SyncSourceRetrier} that delegates on the topology knowledge but, unlike
 * {@link TopologySyncSourceProvider}, it integrates a specially designed {@link Retrier} to wait
 * some seconds until the topology is reconfigured.
 */
@Singleton
public class RetrierTopologySyncSourceProvider implements SyncSourceProvider {

  private final TopologySyncSourceProvider delegate;
  private final SyncSourceRetrier retrier;

  @Inject
  public RetrierTopologySyncSourceProvider(TopologySyncSourceProvider delegate,
      SyncSourceRetrier retrier) {
    this.delegate = delegate;
    this.retrier = retrier;
  }

  @Override
  public HostAndPort newSyncSource() throws NoSyncSourceFoundException {
    return call(() -> delegate.newSyncSource());
  }

  @Override
  public HostAndPort newSyncSource(OpTime lastFetchedOpTime) throws
      NoSyncSourceFoundException {
    return call(() -> delegate.newSyncSource(lastFetchedOpTime));
  }

  @Override
  public Optional<HostAndPort> getLastUsedSyncSource() {
    return delegate.getLastUsedSyncSource();
  }

  @Override
  public boolean shouldChangeSyncSource() {
    return delegate.shouldChangeSyncSource();
  }

  private final HostAndPort call(Callable<HostAndPort> callable) throws NoSyncSourceFoundException {
    try {
      return retrier.retry(callable, Hint.TIME_SENSIBLE);
    } catch (RetrierGiveUpException ex) {
      Throwable cause = ex.getCause();
      if (cause != null && cause instanceof NoSyncSourceFoundException) {
        throw (NoSyncSourceFoundException) cause;
      }
      throw new UncheckedException(ex);
    }
  }

}
