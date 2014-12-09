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

package com.torodb.torod.core.cursors;

import com.torodb.torod.core.Session;

/**
 *
 */
public class CursorProperties {

    private final CursorId id;
    private final boolean hasTimeout;
    private final int limit;
    private final boolean autoclose;

    public CursorProperties(
            CursorId id,
            boolean hasTimeout,
            boolean autoclose) {
        this.id = id;
        this.hasTimeout = hasTimeout;

        this.limit = -1;

        this.autoclose = autoclose;
    }

    public CursorProperties(
            CursorId id,
            boolean hasTimeout,
            int limit,
            boolean autoclose) {
        this.id = id;
        this.hasTimeout = hasTimeout;

        if (limit <= 0) {
            throw new IllegalArgumentException("Limit must be positive, but " + limit);
        }
        this.limit = limit;

        this.autoclose = autoclose;
    }

    public CursorId getId() {
        return id;
    }

    public boolean hasTimeout() {
        return hasTimeout;
    }

    public boolean isAutoclose() {
        return autoclose;
    }

    public boolean hasLimit() {
        return limit > 0;
    }

    /**
     *
     * @return the limit of the cursor or an exception if it doesn't have a limit
     * @throws IllegalStateException when the cursor doesn't have a {@linkplain #hasLimit() limit}
     */
    public int getLimit() throws IllegalStateException {
        if (!hasLimit()) {
            throw new IllegalStateException("This cursor doesn't have a limit, so this method cannot be called");
        }
        return limit;
    }

//    @Override
//    public int hashCode() {
//        int hash = 3;
//        hash = 47 * hash + (this.id != null ? this.id.hashCode() : 0);
//        return hash;
//    }
//
//    @Override
//    public boolean equals(Object obj) {
//        if (obj == null) {
//            return false;
//        }
//        if (getClass() != obj.getClass()) {
//            return false;
//        }
//        final CursorProperties other = (CursorProperties) obj;
//        if (this.id != other.id && (this.id == null || !this.id.equals(other.id))) {
//            return false;
//        }
//        if (this.owner != other.owner && (this.owner == null || !this.owner.equals(other.owner))) {
//            return false;
//        }
//        if (this.hasTimeout != other.hasTimeout) {
//            return false;
//        }
//        if (this.limit != other.limit) {
//            return false;
//        }
//        if (this.autoclose != other.autoclose) {
//            return false;
//        }
//        return true;
//    }

}
