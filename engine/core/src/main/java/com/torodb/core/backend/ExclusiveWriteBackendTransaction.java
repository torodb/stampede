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

import com.torodb.core.exceptions.InvalidDatabaseException;
import com.torodb.core.transaction.RollbackException;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MutableMetaCollection;
import com.torodb.core.transaction.metainf.MutableMetaDatabase;

public interface ExclusiveWriteBackendTransaction extends WriteBackendTransaction {

  /**
   * Rename an existing collection.
   *
   * @param fromDb   the database that contains the collection to rename.
   * @param fromColl the collection to rename.
   * @param toDb     the database that will contain the renamed collection.
   * @param toColl   the renamed collection.
   * @throws BackendException
   * @throws RollbackException
   */
  public void renameCollection(MetaDatabase fromDb, MetaCollection fromColl,
      MutableMetaDatabase toDb, MutableMetaCollection toColl) throws RollbackException;

  /**
   * Drops all torodb elements from the backend, including metatables and their content.
   * <p>
   * After calling this method, ToroDB cannot use the underlying backend until metada is created
   * again.
   */
  public void dropAll() throws RollbackException;

  /**
   * Drops all user elements from the backend, including metatables content but not metatables.
   * <p>
   * After calling this method, ToroDB sees the underlying backend as a fresh system, simmilar to
   * the one that is present the first time ToroDB starts.
   */
  public void dropUserData() throws RollbackException;

  public void checkOrCreateMetaDataTables() throws InvalidDatabaseException;

  @Override
  public void close();
}
