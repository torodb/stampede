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

/**
 *
 */
public class FalseDatabaseQuery implements DatabaseQuery {
    private static final FalseDatabaseQuery INSTANCE = new FalseDatabaseQuery();

    private FalseDatabaseQuery() {
    }
    
    public static FalseDatabaseQuery getInstance() {
        return INSTANCE;
    }
    
    @Override
    public boolean equals(Object object) {
        return object != null && object.getClass().equals(getClass());
    }

    @Override
    public int hashCode() {
        int hash = 7;
        return hash;
    }

    @Override
    public <Result, Arg> Result accept(DatabaseQueryVisitor<Result, Arg> visitor, Arg argument) {
        return visitor.visit(this, argument);
    }
}
