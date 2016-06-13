/*
 *     This file is part of ToroDB.
 *
 *     ToroDB is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Affero General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     ToroDB is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Affero General Public License for more details.
 *
 *     You should have received a copy of the GNU Affero General Public License
 *     along with ToroDB. If not, see <http://www.gnu.org/licenses/>.
 *
 *     Copyright (c) 2014, 8Kdata Technology
 *     
 */

package com.torodb.core.backend;

import com.torodb.core.d2r.DocPartData;
import com.torodb.core.transaction.BackendException;
import com.torodb.core.transaction.RollbackException;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.core.transaction.metainf.MetaField;

public interface BackendConnection {

    /**
     * Adds a new database.
     *
     * @param db         the database to add.
     * @throws BackendException
     * @throws RollbackException
     */
    public void addDatabase(MetaDatabase db) throws BackendException, RollbackException;

    /**
     * Adds a collection to a database.
     *
     * @param db         the database where the collection will be added. It must have been added
     *                   before.
     * @param newCol     the collection to add
     * @throws BackendException
     * @throws RollbackException
     */
    public void addCollection(MetaDatabase db, MetaCollection newCol) 
            throws BackendException, RollbackException;

    /**
     * Adds a docPart to a collection.
     *
     * @param db         the database that contains the given collection. It must have been added
     *                   before.
     * @param col        the collection where the doc part will be added. It must have been added
     *                   before
     * @param newDocPart the docPart to add
     * @throws BackendException
     * @throws RollbackException
     */
    public void addDocPart(MetaDatabase db, MetaCollection col, MetaDocPart newDocPart) 
            throws BackendException, RollbackException;

    /**
     * Adds a field to a table.
     *
     * @param db       the database that contains the given collection. It must have been added
     *                 before
     * @param col      the collection that contains the given docPart. It must have been added
     *                 before
     * @param docPart  the docPart where the field will be added. It must have been added before
     * @param newField the field to add
     * @throws BackendException
     * @throws RollbackException
     */
    public void addField(MetaDatabase db, MetaCollection col, MetaDocPart docPart, MetaField newField)
            throws BackendException, RollbackException;

    /**
     * Reserves a given number of rids on the given doc part.
     * 
     * @param db      the database that contains the given collection
     * @param col     the collection that contains the given doc part
     * @param docPart the doc part where rid want to be consumed
     * @param howMany how many rids want to be consumed.
     * @return the first rid that can be used.
     * @throws BackendException
     * @throws RollbackException
     */
    public int consumeRids(MetaDatabase db, MetaCollection col, MetaDocPart docPart, int howMany)
            throws BackendException, RollbackException;

    /**
     *
     * @param db   the database that contains the given collection
     * @param col  the collection that contains the given data
     * @param data the rows to be inserted
     * @throws BackendException
     * @throws RollbackException
     */
    public void insert(MetaDatabase db, MetaCollection col, DocPartData data) throws BackendException, RollbackException;
}
