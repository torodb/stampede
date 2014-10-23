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

import com.torodb.torod.core.language.querycriteria.utils.QueryCriteriaVisitor;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import javax.annotation.concurrent.Immutable;

/**
 *
 */
@Immutable
public class NotQueryCriteria extends QueryCriteria {
    private static final long serialVersionUID = 1L;

    private final QueryCriteria subCriteria;

    public NotQueryCriteria(QueryCriteria subCriteria) {
        this.subCriteria = subCriteria;
    }

    public QueryCriteria getSubQueryCriteria() {
        return subCriteria;
    }

    @Override
    public String toString() {
        return "not (" +subCriteria + ')';
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 23 * hash + (this.subCriteria != null ? this.subCriteria.hashCode() : 0);
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
        final NotQueryCriteria other = (NotQueryCriteria) obj;
        if (this.subCriteria != other.subCriteria && (this.subCriteria == null || !this.subCriteria.semanticEquals(other.subCriteria))) {
            return false;
        }
        return true;
    }
    
    public <Result, Arg> Result accept(QueryCriteriaVisitor<Result, Arg> visitor, Arg arg) {
        return visitor.visit(this, arg);
    }
    
}
