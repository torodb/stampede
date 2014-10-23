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
import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

/**
 *
 */
@Immutable
public class AndQueryCriteria extends QueryCriteria {

    private static final long serialVersionUID = 1L;

    private final QueryCriteria subQueryCriteria1;
    private final QueryCriteria subQueryCriteria2;

    public AndQueryCriteria(@Nonnull QueryCriteria subQueryCriteria1, @Nonnull QueryCriteria subQueryCriteria2) {
        this.subQueryCriteria1 = subQueryCriteria1;
        this.subQueryCriteria2 = subQueryCriteria2;
    }

    public QueryCriteria getSubQueryCriteria1() {
        return subQueryCriteria1;
    }

    public QueryCriteria getSubQueryCriteria2() {
        return subQueryCriteria2;
    }

    @Override
    public String toString() {
        return "(" + subQueryCriteria1 + ") and (" + subQueryCriteria2 + ')';
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 17 * hash + (this.subQueryCriteria1 != null ? this.subQueryCriteria1.hashCode() : 0);
        hash = 17 * hash + (this.subQueryCriteria2 != null ? this.subQueryCriteria2.hashCode() : 0);
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
        final AndQueryCriteria other = (AndQueryCriteria) obj;
        if (this.subQueryCriteria1 != other.subQueryCriteria1 && (this.subQueryCriteria1 == null || !this.subQueryCriteria1.semanticEquals(other.subQueryCriteria1))) {
            return false;
        }
        if (this.subQueryCriteria2 != other.subQueryCriteria2 && (this.subQueryCriteria2 == null || !this.subQueryCriteria2.semanticEquals(other.subQueryCriteria2))) {
            return false;
        }
        return true;
    }

    @Override
    public <Result, Arg> Result accept(QueryCriteriaVisitor<Result, Arg> visitor, Arg arg) {
        return visitor.visit(this, arg);
    }
}
