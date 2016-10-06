
package com.torodb.core.transaction.metainf;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import com.torodb.core.TableRef;
import com.torodb.core.annotations.DoNotChange;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

/**
 *
 */
public class ImmutableMetaDocPart implements MetaDocPart {

    private final TableRef tableRef;
    private final String identifier;
    private final Table<String, FieldType, ImmutableMetaField> fieldsByNameAndType;
    private final Map<String, ImmutableMetaField> fieldsByIdentifier;
    private final EnumMap<FieldType, ImmutableMetaScalar> scalars;
    private final Map<String, ImmutableMetaDocPartIndex> indexesByIdentifier;

    public ImmutableMetaDocPart(TableRef tableRef, String dbName) {
        this(tableRef, dbName, Collections.emptyMap(), Maps.newEnumMap(FieldType.class), Collections.emptyMap());
    }

    public ImmutableMetaDocPart(TableRef tableRef, String dbName,
            @DoNotChange Map<String, ImmutableMetaField> columns,
            @DoNotChange EnumMap<FieldType, ImmutableMetaScalar> scalars,
            @DoNotChange Map<String, ImmutableMetaDocPartIndex> indexes) {
        this.tableRef = tableRef;
        this.identifier = dbName;
        this.fieldsByIdentifier = columns;
        this.fieldsByNameAndType = HashBasedTable.create(columns.size(), 10);
        columns.values().forEach((column) -> fieldsByNameAndType.put(column.getName(), column.getType(), column));
        this.scalars = scalars;
        this.indexesByIdentifier = indexes;
    }

    @Override
    public TableRef getTableRef() {
        return tableRef;
    }

    @Override
    public String getIdentifier() {
        return identifier;
    }

    @Override
    public Stream<ImmutableMetaField> streamFields() {
        return fieldsByIdentifier.values().stream();
    }

    @Override
    public ImmutableMetaField getMetaFieldByIdentifier(String columnDbName) {
        return fieldsByIdentifier.get(columnDbName);
    }

    @Override
    public Stream<ImmutableMetaField> streamMetaFieldByName(String columnDocName) {
        return fieldsByNameAndType.row(columnDocName).values().stream();
    }

    @Override
    public ImmutableMetaField getMetaFieldByNameAndType(String columnDocName, FieldType type) {
        return fieldsByNameAndType.get(columnDocName, type);
    }

    @Override
    public Stream<ImmutableMetaScalar> streamScalars() {
        return scalars.values().stream();
    }

    @Override
    public ImmutableMetaScalar getScalar(FieldType type) {
        return scalars.get(type);
    }

    @Override
    public Stream<ImmutableMetaDocPartIndex> streamIndexes() {
        return indexesByIdentifier.values().stream();
    }

    @Override
    public ImmutableMetaDocPartIndex getMetaDocPartIndexByIdentifier(String indexName) {
        return indexesByIdentifier.get(indexName);
    }

    @Override
    public String toString() {
        return defautToString();
    }
    
    public static class Builder {

        private boolean built = false;
        private final TableRef tableRef;
        private final String identifier;
        private final HashMap<String, ImmutableMetaField> fields;
        private final EnumMap<FieldType, ImmutableMetaScalar> scalars;
        private final HashMap<String, ImmutableMetaDocPartIndex> indexes;

        public Builder(TableRef tableRef, String identifier) {
            this.tableRef = tableRef;
            this.identifier = identifier;
            this.fields = new HashMap<>();
            this.scalars = new EnumMap<>(FieldType.class);
            this.indexes = new HashMap<>();
        }

        public Builder(ImmutableMetaDocPart other) {
            this.tableRef = other.getTableRef();
            this.identifier = other.getIdentifier();
            this.fields = new HashMap<>(other.fieldsByIdentifier);
            this.scalars = new EnumMap<>(other.scalars);
            this.indexes = new HashMap<>(other.indexesByIdentifier);
        }

        public Builder(TableRef tableRef, String identifier, int expectedColumns, int expectedIndexes) {
            this.tableRef = tableRef;
            this.identifier = identifier;
            this.fields = new HashMap<>(expectedColumns);
            this.scalars = new EnumMap<>(FieldType.class);
            this.indexes = new HashMap<>(expectedIndexes);
        }

        public Builder put(ImmutableMetaField column) {
            Preconditions.checkState(!built, "This builder has already been built");
            fields.put(column.getIdentifier(), column);
            return this;
        }

        public Builder putField(String name, String identifier, FieldType type) {
            return put(new ImmutableMetaField(name, identifier, type));
        }

        public Builder put(ImmutableMetaScalar scalar) {
            scalars.put(scalar.getType(), scalar);
            return this;
        }

        public Builder putScalar(FieldType type, String identifier) {
            scalars.put(type, new ImmutableMetaScalar(identifier, type));
            return this;
        }

        public Builder put(ImmutableMetaDocPartIndex.Builder indexBuilder) {
            return put(indexBuilder.build());
        }

        public Builder put(ImmutableMetaDocPartIndex index) {
            Preconditions.checkState(!built, "This builder has already been built");
            indexes.put(index.getIdentifier(), index);
            return this;
        }

        public ImmutableMetaDocPart build() {
            Preconditions.checkState(!built, "This builder has already been built");
            built = true;
            return new ImmutableMetaDocPart(tableRef, identifier, fields, scalars, indexes);
        }
    }
}
