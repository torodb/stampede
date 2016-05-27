
package com.torodb.core;

import java.util.Objects;
import java.util.Optional;
import javax.annotation.concurrent.Immutable;

/**
 *
 */
@Immutable
public abstract class TableRef {

    public abstract Optional<TableRef> getParent();

    public abstract String getName();

    @Override
    public int hashCode() {
        return Objects.hash(getName(), getParent());
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

        return getName().equals(otherRef.getName()) && getParent().equals(otherRef.getParent());
    }

}
