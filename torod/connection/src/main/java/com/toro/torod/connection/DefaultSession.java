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

package com.toro.torod.connection;

import com.torodb.torod.core.Session;
import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 * <p>
 */
class DefaultSession implements Session, Serializable {

    private static final long serialVersionUID = 1L;

    private static final AtomicInteger idProvider = new AtomicInteger();
    private final int id;

    DefaultSession() {
        this.id = idProvider.incrementAndGet();
    }

    @Override
    public String toString() {
        return "session-" + id;
    }

    @Override
    public int hashCode() {
//        int hash = 7;
//        hash = 79 * hash + this.id;
//        return hash;
        return this.id;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final DefaultSession other = (DefaultSession) obj;
        if (this.id != other.id) {
            return false;
        }
        return true;
    }

}
