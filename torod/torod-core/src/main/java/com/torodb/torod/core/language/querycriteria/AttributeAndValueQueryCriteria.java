/*
 *     This file is part of ToroDB.
 *
 *     ToroDB is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Affero General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     ToroDB is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Affero General Public License for more details.
 *
 *     You should have received a copy of the GNU Affero General Public License
 *     along with ToroDB. If not, see <http://www.gnu.org/licenses/>.
 *
 *     Copyright (c) 2014, 8Kdata Technology
 *     
 */

package com.torodb.torod.core.language.querycriteria;

import com.torodb.torod.core.language.AttributeReference;
import com.torodb.torod.core.subdocument.values.Value;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import javax.annotation.Nonnull;

/**
 *
 */
public abstract class AttributeAndValueQueryCriteria extends AttributeQueryCriteria {
    private static final long serialVersionUID = 1L;

    private final Value<?> value;

    public AttributeAndValueQueryCriteria(@Nonnull AttributeReference attributeReference, @Nonnull Value<?> value) {
        super(attributeReference);
        this.value = value;
    }

    @Nonnull
    public Value getValue() {
        return value;
    }

    protected abstract int getBaseHash();

    @Override
    public int hashCode() {
        int hash = getBaseHash();
        hash = 17 * hash + this.getValue().hashCode();
        hash = 17 * hash + this.getAttributeReference().hashCode();
        return hash;
    }

    @SuppressFBWarnings("BC_UNCONFIRMED_CAST")
    @Override
    public boolean semanticEquals(QueryCriteria obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final AttributeAndValueQueryCriteria other = (AttributeAndValueQueryCriteria) obj;
        if (this.value != other.value && (this.value == null || !this.value.equals(other.value))) {
            return false;
        }
        if (this.getAttributeReference() != other.getAttributeReference() && !this.getAttributeReference().equals(other.getAttributeReference())) {
            return false;
        }
        return true;
    }
}
