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
import com.torodb.torod.core.subdocument.values.Value;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import javax.annotation.Nonnull;

/**
 *
 */
public class ModIsQueryCriteria extends AttributeQueryCriteria {
    private static final long serialVersionUID = 1L;

    @Nonnull
    private final Value<? extends Number> divisor;
    @Nonnull
    private final Value<? extends Number> reminder;

    public ModIsQueryCriteria(AttributeReference attributeReference, Value<? extends Number> divisor, Value<? extends Number> reminder) {
        super(attributeReference);
        this.divisor = divisor;
        this.reminder = reminder;
    }

    @Nonnull
    public Value<? extends Number> getDivisor() {
        return divisor;
    }

    @Nonnull
    public Value<? extends Number> getReminder() {
        return reminder;
    }

    @Override
    public String toString() {
        return getAttributeReference() + " % " + getDivisor() + " = " + getReminder();
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 47 * hash + this.getAttributeReference().hashCode();
        hash = 47 * hash + this.divisor.hashCode();
        hash = 47 * hash + this.reminder.hashCode();
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
        final ModIsQueryCriteria other = (ModIsQueryCriteria) obj;
        if (!this.getAttributeReference().equals(other.getAttributeReference())) {
            return false;
        }
        if (this.divisor != other.divisor && !this.divisor.equals(other.divisor)) {
            return false;
        }
        if (this.reminder != other.reminder && !this.reminder.equals(other.reminder)) {
            return false;
        }
        return true;
    }
    
    @Override
    public <Result, Arg> Result accept(QueryCriteriaVisitor<Result, Arg> visitor, Arg arg) {
        return visitor.visit(this, arg);
    }

}
