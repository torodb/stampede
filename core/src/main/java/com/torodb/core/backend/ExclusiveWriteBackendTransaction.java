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

import com.torodb.core.transaction.RollbackException;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MutableMetaCollection;
import com.torodb.core.transaction.metainf.MutableMetaDatabase;

public interface ExclusiveWriteBackendTransaction extends WriteBackendTransaction {

    /**
     * Rename an existing collection.
     *
     * @param fromDb     the database that contains the collection to rename.
     * @param fromColl the collection to rename.
     * @param toDb     the database that will contain the renamed collection.
     * @param toColl the renamed collection.
     * @throws BackendException
     * @throws RollbackException
     */
    public void renameCollection(MetaDatabase fromDb, MetaCollection fromColl, MutableMetaDatabase toDb, MutableMetaCollection toColl) throws RollbackException;

    @Override
    public void close();
}
