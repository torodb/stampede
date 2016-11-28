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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalNotification;
import com.google.inject.Singleton;
import com.torodb.core.services.IdleTorodbService;
import com.torodb.torod.TorodConnection;
import com.torodb.torod.TorodServer;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import javax.inject.Inject;

/**
 *
 */
@Singleton
public class MemoryTorodServer extends IdleTorodbService implements TorodServer {

  private final MemoryData data = new MemoryData();
  private final AtomicInteger connIdGenerator = new AtomicInteger();
  private final Cache<Integer, MemoryTorodConnection> openConnections;

  @Inject
  public MemoryTorodServer(ThreadFactory threadFactory) {
    super(threadFactory);

    openConnections = CacheBuilder.newBuilder()
        .weakValues()
        .removalListener(this::onConnectionInvalidated)
        .build();
  }

  @Override
  public TorodConnection openConnection() {
    return new MemoryTorodConnection(this, connIdGenerator.incrementAndGet());
  }

  @Override
  public void disableDataImportMode() {
  }

  @Override
  public void enableDataImportMode() {
  }

  @Override
  protected void startUp() throws Exception {
  }

  @Override
  protected void shutDown() throws Exception {
    openConnections.invalidateAll();
    try (MemoryData.MdWriteTransaction trans = data.openWriteTransaction()) {
      trans.clear();
    }
  }

  MemoryData getData() {
    return data;
  }

  private void onConnectionInvalidated(
      RemovalNotification<Integer, MemoryTorodConnection> notification) {
    MemoryTorodConnection value = notification.getValue();
    if (value != null) {
      value.close();
    }
  }

  void onConnectionClosed(MemoryTorodConnection connection) {
    openConnections.invalidate(connection.getConnectionId());
  }

}
