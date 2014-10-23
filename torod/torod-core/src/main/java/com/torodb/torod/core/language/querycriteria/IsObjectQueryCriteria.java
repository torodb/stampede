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

/**
 *
 */
public class IsObjectQueryCriteria extends AttributeQueryCriteria {
    private static final long serialVersionUID = 1L;

    public IsObjectQueryCriteria(AttributeReference attributeReference) {
        super(attributeReference);
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 13 * hash + getAttributeReference().hashCode();
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
        final IsObjectQueryCriteria other = (IsObjectQueryCriteria) obj;
        
        return getAttributeReference().equals(other.getAttributeReference());
    }


    @Override
    public <Result, Arg> Result accept(QueryCriteriaVisitor<Result, Arg> visitor, Arg arg) {
        return visitor.visit(this, arg);
    }
    
}
