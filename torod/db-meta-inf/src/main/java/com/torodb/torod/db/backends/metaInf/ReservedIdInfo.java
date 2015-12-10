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

package com.torodb.torod.db.backends.metaInf;

import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.concurrent.ThreadSafe;

/**
 *
 */
@ThreadSafe
public class ReservedIdInfo {
    private final AtomicInteger lastUsedId;
    private final AtomicInteger lastCachedId;

    public ReservedIdInfo(int lastUsedId, int lastCachedId) {
        this.lastUsedId = new AtomicInteger(lastUsedId);
        this.lastCachedId = new AtomicInteger(lastCachedId);
    }
    
    public void setLastUsedId(int lastUsedId) {
        this.lastUsedId.set(lastUsedId);
    }

    public int getLastUsedId() {
        return lastUsedId.get();
    }
    
    public int getAndAddLastUsedId(int increment) {
        return lastUsedId.getAndAdd(increment);
    }

    public void setLastCachedId(int lastCachedId) {
        this.lastCachedId.set(lastCachedId);
    }

    public int getLastCachedId() {
        return lastCachedId.get();
    }
    
    public int getAndAddLastCachedId(int increment) {
        return lastCachedId.getAndAdd(increment);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }
    
    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final ReservedIdInfo other = (ReservedIdInfo) obj;
        if (this.lastUsedId != other.lastUsedId && (this.lastUsedId == null || !this.lastUsedId.equals(other.lastUsedId))) {
            return false;
        }
        if (this.lastCachedId != other.lastCachedId && (this.lastCachedId == null || !this.lastCachedId.equals(other.lastCachedId))) {
            return false;
        }
        return true;
    }

    
}
