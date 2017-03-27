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

package com.torodb.core.backend;

import com.google.common.util.concurrent.Service;
import com.torodb.common.util.Empty;
import com.torodb.core.transaction.RollbackException;
import com.torodb.core.transaction.metainf.MetaDatabase;

import java.util.concurrent.CompletableFuture;

public interface BackendService extends Service {

  public BackendConnection openConnection();

  /**
   * Disables the data import mode, setting the normal one.
   *
   * <p>This method can be quite slow, as it is usual to execute quite expensive low level task like
   * recreate indexes.
   */
  public CompletableFuture<Empty> disableDataImportMode(MetaDatabase metaDb)
      throws RollbackException;

  /**
   * Sets the backend on a state where inserts are faster.
   *
   * <p/> During this state, only metadata operations and inserts are supported (but it is not
   * mandatory to throw an exception if other operations are recived). It is expected that each
   * call to this method is follow by a call to
   * {@link #enableDataImportMode(com.torodb.core.transaction.metainf.MetaSnapshot,
   * com.torodb.core.transaction.metainf.MetaDatabase) },
   * which will enable the default mode.
   */
  public CompletableFuture<Empty> enableDataImportMode(MetaDatabase metaDb)
      throws RollbackException;
}
