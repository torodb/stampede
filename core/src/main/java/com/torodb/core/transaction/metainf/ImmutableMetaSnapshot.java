package com.torodb.core.transaction.metainf;

import com.google.common.base.Preconditions;
import com.torodb.core.annotations.DoNotChange;
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

    public ImmutableMetaSnapshot(@DoNotChange Map<String, ImmutableMetaDatabase> dbsById) {
        this.dbsByIdentifier = Collections.unmodifiableMap(dbsById);
        this.dbsByName = new HashMap<>(dbsById.size());
        for (ImmutableMetaDatabase schema : dbsById.values()) {
            dbsByName.put(schema.getName(), schema);
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
        return dbsByIdentifier.get(schemaDbName);
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

        public Builder put(ImmutableMetaDatabase.Builder dbBuilder) {
            return Builder.this.put(dbBuilder.build());
        }

        public Builder put(ImmutableMetaDatabase db) {
            Preconditions.checkState(!built, "This builder has already been built");
            dbsByIdentifier.put(db.getIdentifier(), db);
            return this;
        }
        
        public Builder remove(MetaDatabase metaDb) {
            Preconditions.checkState(!built, "This builder has already been built");
            dbsByIdentifier.remove(metaDb.getIdentifier());
            return this;
        }

        public ImmutableMetaSnapshot build() {
            Preconditions.checkState(!built, "This builder has already been built");
            built = true;
            return new ImmutableMetaSnapshot(dbsByIdentifier);
        }
    }
}
