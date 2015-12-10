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

package com.torodb.torod.db.backends.query;

import com.google.common.collect.Maps;
import com.torodb.torod.core.language.querycriteria.ExistsQueryCriteria;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Instances of this class connects elementes that fulfil a structure exist
 * query with a given data exist query.
 */
public class ExistRelation {
    private final Map<ExistsQueryCriteria, ExistsQueryCriteria> relation;

    public ExistRelation() {
        relation = Maps.newHashMap();
    }

    @Nullable
    public ExistsQueryCriteria getStructureExist(ExistsQueryCriteria databaseExist) {
        return relation.get(databaseExist);
    }
    
    public boolean isEmpty() {
        return relation.isEmpty();
    }
    
    public Set<ExistsQueryCriteria> getAllDatabaseQueries() {
        return Collections.unmodifiableSet(relation.keySet());
    }

    public void addRelation(ExistsQueryCriteria databaseExists, ExistsQueryCriteria strucureCriteria) {
        if (relation.containsKey(databaseExists)) {
            throw new IllegalArgumentException("The database query '" + databaseExists + "' is related with the " + "structure query '" + relation.get(databaseExists) + "', so it cannot be associated with the new " + "structure query '" + strucureCriteria + "'");
        }
        relation.put(databaseExists, strucureCriteria);
    }
    
}
