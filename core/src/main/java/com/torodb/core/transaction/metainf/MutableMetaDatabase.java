/*
 * This file is part of ToroDB.
 *
 * ToroDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * ToroDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with core. If not, see <http://www.gnu.org/licenses/>.
 *
 * Copyright (C) 2016 8Kdata.
 * 
 */

package com.torodb.core.transaction.metainf;

import java.util.Collections;

/**
 *
 */
public interface MutableMetaDatabase<MMD extends MutableMetaDatabase> extends MetaDatabase<MetaCollection> {

    public default MMD addCollection(String colName, String colId) {
        return addCollection(colName, colId, Collections.emptyList());
    }

    public abstract MMD addCollection(String colName, String colId, Iterable<ImmutableMetaField> fields);

    /**
     *
     * @param colName
     * @return
     * @throws IllegalArgumentException if {@link #getMetaCollectionByName(java.lang.String)}
     *                                  returns null using the same name
     */
    public abstract MMD asMutableCollection(String colName) throws IllegalArgumentException;

}
