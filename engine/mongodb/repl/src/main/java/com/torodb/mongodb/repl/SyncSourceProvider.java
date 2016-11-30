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

package com.torodb.mongodb.repl;

import com.eightkdata.mongowp.OpTime;
import com.google.common.net.HostAndPort;
import com.torodb.mongodb.repl.exceptions.NoSyncSourceFoundException;

import java.util.Optional;

/**
 *
 */
public interface SyncSourceProvider {

  public HostAndPort newSyncSource() throws NoSyncSourceFoundException;

  /**
   * Returns the host and port of the server that must be used to read from.
   * <p>
   * The host and port is decided by the using the given optime and the old reader.
   * <p>
   * @param lastFetchedOpTime the optime of the last fetched operation
   * @return
   * @throws NoSyncSourceFoundException iff there is no sync source we can reply from using the
   *                                    given optime
   */
  public default HostAndPort newSyncSource(OpTime lastFetchedOpTime) throws
      NoSyncSourceFoundException {
    return newSyncSource();
  }

  public Optional<HostAndPort> getLastUsedSyncSource();

  public boolean shouldChangeSyncSource();

}
