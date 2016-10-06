
package com.torodb.core.transaction.metainf;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.torodb.core.TableRef;
import com.torodb.core.annotations.DoNotChange;

/**
 *
 */
public class ImmutableMetaDocPartIndex implements MetaDocPartIndex {

    private final String identifier;
    private final boolean unique;
    private final Table<String, FieldType, ImmutableMetaFieldIndex> fieldsByNameAndType;
    private final List<ImmutableMetaFieldIndex> fieldsByPosition;

    public ImmutableMetaDocPartIndex(String identifier,  boolean unique) {
        this(identifier, unique, Collections.emptyList());
    }

    public ImmutableMetaDocPartIndex(String identifier, boolean unique,
            @DoNotChange List<ImmutableMetaFieldIndex> columns) {
        this.identifier = identifier;
        this.unique = unique;
        this.fieldsByPosition = columns;
        this.fieldsByNameAndType = HashBasedTable.create(columns.size(), 10);
        columns.forEach((column) -> fieldsByNameAndType.put(column.getName(), column.getType(), column));
    }

    @Override
    public String getIdentifier() {
        return identifier;
    }

    @Override
    public boolean isUnique() {
        return unique;
    }

    @Override
    public int size() {
        return fieldsByPosition.size();
    }

    @Override
    public Stream<ImmutableMetaFieldIndex> streamFields() {
        return fieldsByPosition.stream();
    }

    @Override
    public ImmutableMetaFieldIndex getMetaFieldIndexByPosition(int position) {
        return fieldsByPosition.get(position);
    }

    @Override
    public ImmutableMetaFieldIndex getMetaFieldIndexByNameAndType(String columnDocName, FieldType type) {
        return fieldsByNameAndType.get(columnDocName, type);
    }

    @Override
    public String toString() {
        return defautToString();
    }
    
    public static class Builder {

        private boolean built = false;
        private final String identifier;
        private final boolean unique;
        private final ArrayList<ImmutableMetaFieldIndex> fields;

        public Builder(TableRef tableRef, String identifier, boolean unique) {
            this.identifier = identifier;
            this.unique = unique;
            this.fields = new ArrayList<>();
        }

        public Builder(ImmutableMetaDocPartIndex other) {
            this.identifier = other.getIdentifier();
            this.unique = other.isUnique();
            this.fields = new ArrayList<>(other.fieldsByPosition);
        }

        public Builder(TableRef tableRef, String identifier, boolean unique, int expectedColumns) {
            this.identifier = identifier;
            this.unique = unique;
            this.fields = new ArrayList<>(expectedColumns);
        }

        public Builder add(ImmutableMetaFieldIndex column) {
            Preconditions.checkState(!built, "This builder has already been built");
            fields.add(column);
            return this;
        }

        public Builder addField(String name, FieldType type, FieldIndexOrdering ordering) {
            return add(new ImmutableMetaFieldIndex(fields.size(), name, type, ordering));
        }

        public ImmutableMetaDocPartIndex build() {
            Preconditions.checkState(!built, "This builder has already been built");
            built = true;
            return new ImmutableMetaDocPartIndex(identifier, unique, fields);
        }
    }

}
