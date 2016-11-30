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

import com.torodb.core.d2r.DocPartData;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.transaction.RollbackException;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.core.transaction.metainf.MetaField;
import com.torodb.core.transaction.metainf.MetaIdentifiedDocPartIndex;
import com.torodb.core.transaction.metainf.MetaIndex;
import com.torodb.core.transaction.metainf.MetaScalar;
import com.torodb.core.transaction.metainf.MutableMetaCollection;
import com.torodb.core.transaction.metainf.MutableMetaDocPart;
import com.torodb.kvdocument.values.KvValue;

import java.util.Collection;

public interface WriteBackendTransaction extends BackendTransaction {

  /**
   * Adds a new database.
   *
   * @param db the database to add.
   * @throws RollbackException
   */
  public void addDatabase(MetaDatabase db) throws RollbackException;

  /**
   * Adds a collection to a database.
   *
   * @param db     the database where the collection will be added. It must not have been added
   *               before.
   * @param newCol the collection to add
   * @throws RollbackException
   */
  public void addCollection(MetaDatabase db, MetaCollection newCol) throws RollbackException;

  /**
   * Drop an existing collection.
   *
   * @param db   the database that contains the collection to drop.
   * @param coll the collection to drop.
   * @throws RollbackException
   */
  public void dropCollection(MetaDatabase db, MetaCollection coll) throws RollbackException;

  /**
   * Drop an existing database.
   *
   * @param db the database to drop.
   * @throws RollbackException
   */
  public void dropDatabase(MetaDatabase db) throws RollbackException;

  /**
   * Adds a docPart to a collection.
   *
   * Contained {@link MetaDocPart#streamFields() fields} and
   * {@link MetaDocPart#streamScalars() () scalars} <b>are not</b> added and they must be added
   * later calling {@link #addField(com.torodb.core.transaction.metainf.MetaDatabase,
   * com.torodb.core.transaction.metainf.MetaCollection,
   * com.torodb.core.transaction.metainf.MetaDocPart,
   * com.torodb.core.transaction.metainf.MetaField) } and {@link #addScalar(
   * com.torodb.core.transaction.metainf.MetaDatabase,
   * com.torodb.core.transaction.metainf.MetaCollection,
   * com.torodb.core.transaction.metainf.MetaDocPart,
   * com.torodb.core.transaction.metainf.MetaScalar)
   * }
   *
   * @param db         the database that contains the given collection. It must have been added
   *                   before.
   * @param col        the collection where the doc part will be added. It must have been added
   *                   before
   * @param newDocPart the docPart to add
   * @throws RollbackException
   */
  public void addDocPart(MetaDatabase db, MetaCollection col, MetaDocPart newDocPart) throws
      RollbackException;

  /**
   * Adds a field to a table. Create a {@link MetaIdentifiedDocPartIndex physical index} if
   * compatible with an existing {@link MetaIndex logical index}.
   *
   * @param db       the database that contains the given collection. It must have been added before
   * @param col      the collection that contains the given docPart. It must have been added before
   * @param docPart  the docPart where the field will be added. It must have been added before
   * @param newField the field to add
   * @throws RollbackException
   */
  public void addField(MetaDatabase db, MetaCollection col, MutableMetaDocPart docPart,
      MetaField newField) throws UserException, RollbackException;

  /**
   * @param db        the database that contains the given collection. It must have been added
   *                  before
   * @param col       the collection that contains the given docPart. It must have been added before
   * @param docPart   the docPart where the scalar will be added. It must have been added before
   * @param newScalar the scalar to add
   */
  public void addScalar(MetaDatabase db, MetaCollection col, MetaDocPart docPart,
      MetaScalar newScalar);

  /**
   * Reserves a given number of rids on the given doc part.
   *
   * @param db      the database that contains the given collection
   * @param col     the collection that contains the given doc part
   * @param docPart the doc part where rid want to be consumed
   * @param howMany how many rids want to be consumed.
   * @return the first rid that can be used.
   * @throws RollbackException
   */
  public int consumeRids(MetaDatabase db, MetaCollection col, MetaDocPart docPart, int howMany)
      throws RollbackException;

  /**
   *
   * @param db   the database that contains the given collection
   * @param col  the collection that contains the given data
   * @param data the rows to be inserted
   * @throws com.torodb.core.exceptions.user.UserException
   * @throws RollbackException
   */
  public void insert(MetaDatabase db, MetaCollection col, DocPartData data) 
      throws RollbackException, UserException;

  public void deleteDids(MetaDatabase db, MetaCollection col, Collection<Integer> dids);

  /**
   * Create a logical index on doc part. If not yet existing, a physical index will be created for
   * each existent and future doc part fields and scalars that satisfy logical index definition.
   *
   * @param db
   * @param col
   * @param index
   */
  public void createIndex(MetaDatabase db, MutableMetaCollection col, MetaIndex index) 
      throws UserException;

  /**
   * Drop a logical index on doc part. Physical indexes that satisfy logical index definition and
   * are not used by other logical indexes will be dropped.
   *
   * @param db
   * @param col
   * @param index
   */
  public void dropIndex(MetaDatabase db, MutableMetaCollection col, MetaIndex index);

  /**
   * Stores the given key value association.
   *
   * This metainfo is a key-value storage that different modules can use to store their own
   * information.
   *
   * @param key
   * @param newValue
   * @return the old value or null if none was stored
   * @throws IllegalArgumentException if the given key is not registered
   */
  public KvValue<?> writeMetaInfo(MetaInfoKey key, KvValue<?> newValue);

  public void commit() throws UserException, RollbackException;

  @Override
  public void close();
}
