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

import com.google.common.annotations.Beta;
import com.google.common.net.HostAndPort;
import com.torodb.mongodb.repl.SyncSourceProvider;

import java.util.Optional;

import javax.annotation.Nonnull;

/**
 *
 */
@Beta
public class FollowerSyncSourceProvider implements SyncSourceProvider {

  private final HostAndPort syncSource;

  public FollowerSyncSourceProvider(@Nonnull HostAndPort syncSource) {
    this.syncSource = syncSource;
  }

  @Override
  public HostAndPort newSyncSource() {
    return syncSource;
  }

  @Override
  public Optional<HostAndPort> getLastUsedSyncSource() {
    return Optional.of(syncSource);
  }

  @Override
  public boolean shouldChangeSyncSource() {
    return false;
  }

}
