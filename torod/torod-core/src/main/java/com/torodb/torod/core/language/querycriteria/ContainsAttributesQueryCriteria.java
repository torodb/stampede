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
import com.google.common.collect.ImmutableSet;
import com.torodb.torod.core.language.querycriteria.utils.QueryCriteriaVisitor;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Set;

/**
 *
 */
public class ContainsAttributesQueryCriteria extends AttributeQueryCriteria {
    private static final long serialVersionUID = 1L;
    
    private final ImmutableSet<String> attributes;
    private final boolean exclusive;

    public ContainsAttributesQueryCriteria(AttributeReference attributeReference, Iterable<String> attributes, boolean exclusive) {
        super(attributeReference);
        this.attributes = ImmutableSet.copyOf(attributes);
        this.exclusive = exclusive;
    }

    /**
     * It must point to a subdocument structure, not to an array.
     * @return 
     */
    @Override
    public AttributeReference getAttributeReference() {
        return super.getAttributeReference();
    }

    public Set<String> getAttributes() {
        return attributes;
    }

    public boolean isExclusive() {
        return exclusive;
    }

    @Override
    public String toString() {
        return getAttributeReference() + " contains" + attributes + (exclusive ? " (exclusively)" : "");
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 37 * getAttributeReference().hashCode();
        hash = 37 * hash + (this.exclusive ? 1 : 0);
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
        final ContainsAttributesQueryCriteria other = (ContainsAttributesQueryCriteria) obj;
        if (!this.getAttributeReference().equals(other.getAttributeReference())) {
            return false;
        }
        if (this.attributes != other.attributes && (this.attributes == null || !this.attributes.equals(other.attributes))) {
            return false;
        }
        if (this.exclusive != other.exclusive) {
            return false;
        }
        return true;
    }

    @Override
    public <Result, Arg> Result accept(QueryCriteriaVisitor<Result, Arg> visitor, Arg arg) {
        return visitor.visit(this, arg);
    }
}
