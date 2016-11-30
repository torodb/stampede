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
import com.torodb.core.transaction.RollbackException;
import com.torodb.core.transaction.metainf.MetaSnapshot;

/**
 *
 */
public interface BackendService extends Service {

  public BackendConnection openConnection();

  /**
   * Disables the data import mode, setting the normal one.
   *
   * This method can be quite slow, as it is usual to execute quite expensive low level task like
   * recreate indexes.
   *
   * @param snapshot the meta data snapshot.
   * @throws RollbackException
   */
  public void disableDataImportMode(MetaSnapshot snapshot) throws RollbackException;

  /**
   * Sets the backend on a state where inserts are faster.
   *
   * During this state, only metadata operations and inserts are supported (but it is not mandatory
   * to throw an exception if other operations are recived). It is expected that each call to this
   * method is follow by a call to
     * {@link #enableDataImportMode(com.torodb.core.transaction.metainf.MetaSnapshot) }, which will
   * enable the default mode.
   *
   * @param snapshot the meta data snapshot.
   * @throws RollbackException
   */
  public void enableDataImportMode(MetaSnapshot snapshot) throws RollbackException;
}
