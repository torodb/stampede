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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.util.List;

/**
 *
 */
public class OrDatabaseQuery implements DatabaseQuery {

    private final List<DatabaseQuery> children;

    public OrDatabaseQuery(List<DatabaseQuery> children) {
        this.children = ImmutableList.copyOf(children);
    }

    public List<DatabaseQuery> getChildren() {
        return children;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 89 * hash + (this.children != null ? this.children.hashCode() : 0);
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
        final OrDatabaseQuery other = (OrDatabaseQuery) obj;
        if (this.children != other.children && (this.children == null || !this.children.equals(other.children))) {
            return false;
        }
        return true;
    }

    @Override
    public <Result, Arg> Result accept(DatabaseQueryVisitor<Result, Arg> visitor, Arg argument) {
        return visitor.visit(this, argument);
    }
    
    public static class Builder {
        private final List<DatabaseQuery> children;

        public Builder() {
            children = Lists.newArrayList();
        }
        
        public Builder add(DatabaseQuery element) {
            children.add(element);
            return this;
        }
        
        public OrDatabaseQuery build() {
            return new OrDatabaseQuery(children);
        }
    }
}
