
package com.torodb.core.transaction.metainf;

import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

/**
 *
 */
public class ImmutableMetaSnapshot implements MetaSnapshot {

    private final Map<String, ImmutableMetaDatabase> dbsByIdentifier;
    private final Map<String, ImmutableMetaDatabase> dbsByName;

    public ImmutableMetaSnapshot(Iterable<ImmutableMetaDatabase> dbs) {
        Map<String, ImmutableMetaDatabase> byName = new HashMap<>();
        Map<String, ImmutableMetaDatabase> byId = new HashMap<>();
        for (ImmutableMetaDatabase database : dbs) {
            byName.put(database.getName(), database);
            byId.put(database.getIdentifier(), database);
        }
        this.dbsByName = Collections.unmodifiableMap(byName);
        this.dbsByIdentifier = Collections.unmodifiableMap(byId);
    }

    public ImmutableMetaSnapshot(Map<String, ImmutableMetaDatabase> dbsByName) {
        this.dbsByName = Collections.unmodifiableMap(dbsByName);
        this.dbsByIdentifier = new HashMap<>(dbsByName.size());
        for (ImmutableMetaDatabase schema : dbsByName.values()) {
            dbsByIdentifier.put(schema.getIdentifier(), schema);
        }
    }

    @Override
    public Stream<ImmutableMetaDatabase> streamMetaDatabases() {
        return dbsByIdentifier.values().stream();
    }

    @Override
    public ImmutableMetaDatabase getMetaDatabaseByName(String schemaDocName) {
        return dbsByName.get(schemaDocName);
    }

    @Override
    public ImmutableMetaDatabase getMetaDatabaseByIdentifier(String schemaDbName) {
        return dbsByName.get(schemaDbName);
    }

    public static class Builder {
        private boolean built = false;
        private final Map<String, ImmutableMetaDatabase> dbsByIdentifier;

        public Builder() {
            dbsByIdentifier = new HashMap<>();
        }

        public Builder(int expectedDbs) {
            dbsByIdentifier = new HashMap<>(expectedDbs);
        }

        public Builder(ImmutableMetaSnapshot other) {
            this.dbsByIdentifier = new HashMap<>(other.dbsByIdentifier);
        }
        
        public Builder add(ImmutableMetaDatabase.Builder dbBuilder) {
            return add(dbBuilder.build());
        }

        public Builder add(ImmutableMetaDatabase db) {
            Preconditions.checkState(!built, "This builder has already been built");
            dbsByIdentifier.put(db.getIdentifier(), db);
            return this;
        }

        public ImmutableMetaSnapshot build() {
            Preconditions.checkState(!built, "This builder has already been built");
            built = true;
            return new ImmutableMetaSnapshot(dbsByIdentifier);
        }
    }

}
