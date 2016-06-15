package com.torodb.backend.rid;

import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.concurrent.ThreadSafe;

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
