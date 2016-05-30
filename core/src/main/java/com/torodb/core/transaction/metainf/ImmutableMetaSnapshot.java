
package com.torodb.core.transaction.metainf;

import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

/**
 *
 */
public class ImmutableMetaSnapshot implements MetaSnapshot<ImmutableMetaDatabase> {

    private final Map<String, ImmutableMetaDatabase> dbsByIdentifier;
    private final Map<String, ImmutableMetaDatabase> bdsByName;

    public ImmutableMetaSnapshot(Iterable<ImmutableMetaDatabase> dbs) {
        Map<String, ImmutableMetaDatabase> byName = new HashMap<>();
        Map<String, ImmutableMetaDatabase> byId = new HashMap<>();
        for (ImmutableMetaDatabase database : dbs) {
            byName.put(database.getName(), database);
            byId.put(database.getIdentifier(), database);
        }
        this.bdsByName = Collections.unmodifiableMap(byId);
        this.dbsByIdentifier = Collections.unmodifiableMap(byName);
    }

    public ImmutableMetaSnapshot(Map<String, ImmutableMetaDatabase> dbsByName) {
        this.bdsByName = Collections.unmodifiableMap(dbsByName);
        this.dbsByIdentifier = new HashMap<>(dbsByName.size());
        for (ImmutableMetaDatabase schema : dbsByName.values()) {
            dbsByIdentifier.put(schema.getName(), schema);
        }
    }

    @Override
    public Stream<ImmutableMetaDatabase> streamMetaDatabases() {
        return bdsByName.values().stream();
    }

    @Override
    public ImmutableMetaDatabase getMetaDatabaseByName(String schemaDocName) {
        return dbsByIdentifier.get(schemaDocName);
    }

    @Override
    public ImmutableMetaDatabase getMetaDatabaseByIdentifier(String schemaDbName) {
        return bdsByName.get(schemaDbName);
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
