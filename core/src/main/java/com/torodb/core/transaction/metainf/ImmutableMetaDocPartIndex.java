
package com.torodb.core.transaction.metainf;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.torodb.core.annotations.DoNotChange;

/**
 *
 */
public class ImmutableMetaDocPartIndex extends AbstractMetaDocPartIndex {

    private final Map<String, ImmutableMetaDocPartIndexColumn> columnsByIdentifier;
    private final List<ImmutableMetaDocPartIndexColumn> columnsByPosition;

    public ImmutableMetaDocPartIndex(String identifier,  boolean unique) {
        this(identifier, unique, Collections.emptyList());
    }

    public ImmutableMetaDocPartIndex(String identifier, boolean unique,
            @DoNotChange List<ImmutableMetaDocPartIndexColumn> columns) {
        super(identifier, unique);
        this.columnsByPosition = columns;
        this.columnsByIdentifier = new HashMap<>();
        columns.forEach((column) -> columnsByIdentifier.put(column.getIdentifier(), column));
    }

    @Override
    public int size() {
        return columnsByPosition.size();
    }

    @Override
    public Iterator<ImmutableMetaDocPartIndexColumn> iteratorColumns() {
        return columnsByPosition.iterator();
    }

    @Override
    public ImmutableMetaDocPartIndexColumn getMetaDocPartIndexColumnByPosition(int position) {
        return columnsByPosition.get(position);
    }

    @Override
    public ImmutableMetaDocPartIndexColumn getMetaDocPartIndexColumnByIdentifier(String columnName) {
        return columnsByIdentifier.get(columnName);
    }
    
    public static class Builder {

        private boolean built = false;
        private final String identifier;
        private final boolean unique;
        private final ArrayList<ImmutableMetaDocPartIndexColumn> columns;

        public Builder(String identifier, boolean unique) {
            this.identifier = identifier;
            this.unique = unique;
            this.columns = new ArrayList<>();
        }

        public Builder(ImmutableMetaDocPartIndex other) {
            this.identifier = other.getIdentifier();
            this.unique = other.isUnique();
            this.columns = new ArrayList<>(other.columnsByPosition);
        }

        public Builder(AbstractMetaDocPartIndex other) {
            this(other.getIdentifier(), other.isUnique());
        }

        public Builder(String identifier, boolean unique, int expectedColumns) {
            this.identifier = identifier;
            this.unique = unique;
            this.columns = new ArrayList<>(expectedColumns);
        }

        public Builder add(ImmutableMetaDocPartIndexColumn column) {
            Preconditions.checkState(!built, "This builder has already been built");
            Preconditions.checkState(column.getPosition() == columns.size(), 
                    "Column position is %s but was expected to be %s", 
                    column.getPosition(), columns.size());
            columns.add(column);
            return this;
        }

        public Builder addColumn(String identifier, FieldIndexOrdering ordering) {
            return add(new ImmutableMetaDocPartIndexColumn(columns.size(), identifier, ordering));
        }

        public ImmutableMetaDocPartIndex build() {
            Preconditions.checkState(!built, "This builder has already been built");
            built = true;
            return new ImmutableMetaDocPartIndex(identifier, unique, columns);
        }
    }

}
