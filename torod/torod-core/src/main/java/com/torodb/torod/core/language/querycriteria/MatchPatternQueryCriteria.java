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
import java.util.regex.Pattern;
import javax.annotation.Nonnull;

/**
 *
 */
public class MatchPatternQueryCriteria extends AttributeQueryCriteria {
    private static final long serialVersionUID = 1L;

    @Nonnull 
    private final Pattern pattern;

    public MatchPatternQueryCriteria(@Nonnull AttributeReference attributeReference, @Nonnull Pattern pattern) {
        super(attributeReference);
        this.pattern = pattern;
    }

    public Pattern getPattern() {
        return pattern;
    }

    @Override
    public boolean semanticEquals(QueryCriteria obj) {
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof MatchPatternQueryCriteria)) {
            return false;
        }
        final MatchPatternQueryCriteria other = (MatchPatternQueryCriteria) obj;
        if (!this.pattern.equals(other.pattern)) {
            return false;
        }
        return this.getAttributeReference().equals(other.getAttributeReference());
    }

    @Override
    public int hashCode() {
        int hash = 23;
        hash = 17 * hash + pattern.hashCode();
        hash = 17 * hash + this.getAttributeReference().hashCode();
        return hash;
    }

    @Override
    public String toString() {
        return getAttributeReference() + "->matchPattern(" + pattern + ")";
    }
    
    @Override
    public <Result, Arg> Result accept(QueryCriteriaVisitor<Result, Arg> visitor, Arg arg) {
        return visitor.visit(this, arg);
    }

}
