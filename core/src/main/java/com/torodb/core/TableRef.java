
package com.torodb.core;

import com.google.common.base.Objects;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

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
