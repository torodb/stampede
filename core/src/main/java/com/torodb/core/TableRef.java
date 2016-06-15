
package com.torodb.core;

import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import com.google.common.base.Objects;

/**
 *
 */
@Immutable
public abstract class TableRef {

    public abstract Optional<TableRef> getParent();

    /**
     * The name of this TableRef on the document model.
     *
     * For example, the table referenced by "a.b.c" should have the name "c". On any collection, the
     * root TableRef has the empty name as string.
     * @return
     */
    @Nonnull
    public abstract String getName();

    /**
     * The depth of this TableRef on the document model.
     *
     * For example, the table referenced by "a.b.c" should have depth 3. On any collection, the
     * root TableRef has the depth 0.
     * @return
     */
    @Nonnull
    public abstract int getDepth();

    /**
     * The array dimension of this TableRef on the document model.
     *
     * For example, the table referenced by "a.b.c.$2.$3" should have array dimension 3. On any collection, the
     * root TableRef has array dimension 0.
     * @return
     */
    @Nonnull
    public abstract int getArrayDimension();

    /**
     * Indicates if this TableRef has is contained by an array.
     *
     * @return
     */
    @Nonnull
    public abstract boolean isInArray();
    
    public boolean isRoot() {
        return !getParent().isPresent();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        toString(sb);
        
        return sb.toString();
    }

    protected void toString(StringBuilder sb) {
        Optional<TableRef> parent = getParent();
        if (parent.isPresent()) {
            TableRef parentRef = parent.get();
            parentRef.toString(sb);
            if (!parentRef.isRoot()) {
                sb.append('.');
            }
        }
        sb.append(getName());
    }

    @Override
    public int hashCode() {
        return getName().hashCode();
    }
    
    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if (!(other instanceof TableRef)) {
            return false;
        }
        TableRef otherRef = (TableRef) other;

        return getName().equals(otherRef.getName()) && Objects.equal(getParent(), otherRef.getParent());
    }

}
