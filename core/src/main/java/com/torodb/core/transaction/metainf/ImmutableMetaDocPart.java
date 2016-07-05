
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

    public ImmutableMetaDocPart(TableRef tableRef, String dbName) {
        this(tableRef, dbName, Collections.emptyMap(), Maps.newEnumMap(FieldType.class));
    }

    public ImmutableMetaDocPart(TableRef tableRef, String dbName,
            @DoNotChange Map<String, ImmutableMetaField> columns,
            @DoNotChange EnumMap<FieldType, ImmutableMetaScalar> scalars) {
        this.tableRef = tableRef;
        this.identifier = dbName;
        this.fieldsByIdentifier = columns;
        this.fieldsByNameAndType = HashBasedTable.create(columns.size(), 10);
        columns.values().forEach((column) -> fieldsByNameAndType.put(column.getName(), column.getType(), column));
        this.scalars = scalars;
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
    public String toString() {
        return defautToString();
    }
    
    public static class Builder {

        private boolean built = false;
        private final TableRef tableRef;
        private final String identifier;
        private final HashMap<String, ImmutableMetaField> fields;
        private final EnumMap<FieldType, ImmutableMetaScalar> scalars;

        public Builder(TableRef tableRef, String identifier) {
            this.tableRef = tableRef;
            this.identifier = identifier;
            this.fields = new HashMap<>();
            this.scalars = new EnumMap<>(FieldType.class);
        }

        public Builder(ImmutableMetaDocPart other) {
            this.tableRef = other.getTableRef();
            this.identifier = other.getIdentifier();
            this.fields = new HashMap<>(other.fieldsByIdentifier);
            this.scalars = new EnumMap<>(other.scalars);
        }

        public Builder(TableRef tableRef, String identifier, int expectedColumns) {
            this.tableRef = tableRef;
            this.identifier = identifier;
            this.fields = new HashMap<>(expectedColumns);
            this.scalars = new EnumMap<>(FieldType.class);
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

        public ImmutableMetaDocPart build() {
            Preconditions.checkState(!built, "This builder has already been built");
            built = true;
            return new ImmutableMetaDocPart(tableRef, identifier, fields, scalars);
        }
    }
}
