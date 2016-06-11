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


package com.torodb.torod.db.backends.query.dblanguage;

import org.jooq.Record1;
import org.jooq.SelectForUpdateStep;

/**
 *
 */
public class SelectDatabaseQuery implements DatabaseQuery {
    
    private final SelectForUpdateStep<Record1<Integer>> query;

    public SelectDatabaseQuery(SelectForUpdateStep<Record1<Integer>> query) {
        this.query = query;
    }

    public SelectForUpdateStep<Record1<Integer>> getQuery() {
        return query;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 41 * hash + (this.query != null ? this.query.hashCode() : 0);
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
        final SelectDatabaseQuery other = (SelectDatabaseQuery) obj;
        if (this.query != other.query && (this.query == null || !this.query.equals(other.query))) {
            return false;
        }
        return true;
    }

    @Override
    public <Result, Arg> Result accept(DatabaseQueryVisitor<Result, Arg> visitor, Arg argument) {
        return visitor.visit(this, argument);
    }

}
