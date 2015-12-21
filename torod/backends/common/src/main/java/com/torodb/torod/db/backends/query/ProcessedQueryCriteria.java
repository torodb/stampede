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

import com.torodb.torod.core.language.querycriteria.QueryCriteria;
import javax.annotation.Nullable;

/**
 *
 */
public class ProcessedQueryCriteria {
    private final QueryCriteria structureQuery;
    private final QueryCriteria dataQuery;

    public ProcessedQueryCriteria(QueryCriteria structureQuery, QueryCriteria dataQuery) {
        this.structureQuery = structureQuery;
        this.dataQuery = dataQuery;
    }

    /**
     * Returns the query that must be evaluated agains the structure.
     * <p>
     * This query can contain ors.
     * <p>
     * @return
     */
    @Nullable
    public QueryCriteria getStructureQuery() {
        return structureQuery;
    }

    /**
     * Returns the query that must be evaluated agains the database.
     * <p>
     * This query can NOT contain ors.
     * <p>
     * @return
     */
    @Nullable
    public QueryCriteria getDataQuery() {
        return dataQuery;
    }

    @Override
    public String toString() {
        return "s = " + structureQuery + ", d =" + dataQuery;
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 71 * hash + (this.structureQuery != null ? this.structureQuery.hashCode() : 0);
        hash = 71 * hash + (this.dataQuery != null ? this.dataQuery.hashCode() : 0);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final ProcessedQueryCriteria other = (ProcessedQueryCriteria) obj;
        if (this.structureQuery != other.structureQuery && (this.structureQuery == null || !this.structureQuery.equals(other.structureQuery))) {
            return false;
        }
        if (this.dataQuery != other.dataQuery && (this.dataQuery == null || !this.dataQuery.equals(other.dataQuery))) {
            return false;
        }
        return true;
    }

    public boolean semanticEquals(ProcessedQueryCriteria obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final ProcessedQueryCriteria other = (ProcessedQueryCriteria) obj;
        if (this.structureQuery != other.structureQuery && (this.structureQuery == null || !this.structureQuery.semanticEquals(other.structureQuery))) {
            return false;
        }
        if (this.dataQuery != other.dataQuery && (this.dataQuery == null || !this.dataQuery.semanticEquals(other.dataQuery))) {
            return false;
        }
        return true;
    }


}
