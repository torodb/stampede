package com.torodb.core.backend;

public class UpdateResult {
    private final long created;
    private final long modified;
    public UpdateResult(long created, long modified) {
        super();
        this.created = created;
        this.modified = modified;
    }
    public long getCreated() {
        return created;
    }
    public long getModified() {
        return modified;
    }
}
