
package com.torodb.core.transaction.metainf;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.torodb.core.TableRef;
import com.torodb.core.annotations.DoNotChange;
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

    public ImmutableMetaDocPart(TableRef tableRef, String dbName, @DoNotChange Table<String, FieldType, ImmutableMetaField> columns) {
        this.tableRef = tableRef;
        this.identifier = dbName;
        this.fieldsByNameAndType = columns;
        this.fieldsByIdentifier = new HashMap<>(columns.size());
        columns.values().forEach((column) -> fieldsByIdentifier.put(column.getIdentifier(), column));
    }

    public ImmutableMetaDocPart(TableRef tableRef, String dbName, @DoNotChange Map<String, ImmutableMetaField> columns) {
        this.tableRef = tableRef;
        this.identifier = dbName;
        this.fieldsByIdentifier = columns;
        this.fieldsByNameAndType = HashBasedTable.create(columns.size(), 10);
        columns.values().forEach((column) -> fieldsByNameAndType.put(column.getName(), column.getType(), column));
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
    
    public static class Builder {

        private boolean built = false;
        private final TableRef tableRef;
        private final String identifier;
        private final HashMap<String, ImmutableMetaField> fields;

        public Builder(TableRef tableRef, String identifier) {
            this.tableRef = tableRef;
            this.identifier = identifier;
            this.fields = new HashMap<>();
        }

        public Builder(ImmutableMetaDocPart other) {
            this.tableRef = other.getTableRef();
            this.identifier = other.getIdentifier();
            this.fields = new HashMap<>(other.fieldsByIdentifier);
        }

        public Builder(TableRef tableRef, String identifier, int expectedColumns) {
            this.tableRef = tableRef;
            this.identifier = identifier;
            this.fields = new HashMap<>(expectedColumns);
        }

        public Builder add(ImmutableMetaField column) {
            Preconditions.checkState(!built, "This builder has already been built");
            fields.put(column.getIdentifier(), column);
            return this;
        }

        public Builder addColumn(String name, String identifier, FieldType type) {
            return add(new ImmutableMetaField(name, identifier, type));
        }

        public ImmutableMetaDocPart build() {
            Preconditions.checkState(!built, "This builder has already been built");
            built = true;
            return new ImmutableMetaDocPart(tableRef, identifier, fields);
        }
    }
}
