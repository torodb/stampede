
package com.torodb.core.transaction.metainf;

import javax.annotation.concurrent.Immutable;

import com.torodb.core.TableRef;

/**
 *
 */
@Immutable
public class ImmutableMetaIndexField implements MetaIndexField {

    private final int position;
    private final TableRef tableRef;
    private final String name;
    private final FieldIndexOrdering ordering;

    public ImmutableMetaIndexField(int position, TableRef tableRef, String name, FieldIndexOrdering ordering) {
        super();
        this.position = position;
        this.tableRef = tableRef;
        this.name = name;
        this.ordering = ordering;
    }

    @Override
    public int getPosition() {
        return position;
    }

    @Override
    public TableRef getTableRef() {
        return tableRef;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public FieldIndexOrdering getOrdering() {
        return ordering;
    }

    @Override
    public String toString() {
        return defautToString();
    }

}
