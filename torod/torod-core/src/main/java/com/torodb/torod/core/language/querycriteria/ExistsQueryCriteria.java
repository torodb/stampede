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
import com.torodb.torod.core.language.querycriteria.utils.QueryCriteriaVisitor;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

/**
 * .
 * Exists declares an implicit iterator. AttributeReferences used in its body are relative to the elements directly
 * contained by the array pointed by the exists' attribute reference.
 *
 */
@Immutable
public class ExistsQueryCriteria extends AttributeQueryCriteria {
    private static final long serialVersionUID = 1L;
    
    @Nonnull 
    private final QueryCriteria body;

    public ExistsQueryCriteria(@Nonnull AttributeReference attributeReference, @Nonnull QueryCriteria body) {
        super(attributeReference);
        this.body = body;
    }

    @Nonnull
    public QueryCriteria getBody() {
        return body;
    }

    @Override
    public String toString() {
        return getAttributeReference().toString() + " exists (" + body + ')';
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 79 * hash + this.getAttributeReference().hashCode();
        hash = 79 * hash + this.body.hashCode();
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
        final ExistsQueryCriteria other = (ExistsQueryCriteria) obj;
        if (!this.getAttributeReference().equals(other.getAttributeReference())) {
            return false;
        }
        if (this.body != other.body && !this.body.semanticEquals(other.body)) {
            return false;
        }
        return true;
    }
    
    @Override
    public <Result, Arg> Result accept(QueryCriteriaVisitor<Result, Arg> visitor, Arg arg) {
        return visitor.visit(this, arg);
    }
}
