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

package com.torodb.torod.core.subdocument.values;

import com.torodb.torod.core.subdocument.ScalarType;
import org.threeten.bp.Instant;

/**
 *
 */
public abstract class ScalarInstant extends ScalarValue<Instant> {

    private static final long serialVersionUID = 4969815509576158995L;

    @Override
    public ScalarType getType() {
        return ScalarType.INSTANT;
    }

    public long getMillisFromUnix() {
        return getValue().toEpochMilli();
    }

    @Override
    public Class<? extends Instant> getValueClass() {
        return Instant.class;
    }

    @Override
    public String toString() {
        return getValue().toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof ScalarInstant)) {
            return false;
        }
        ScalarInstant other = (ScalarInstant) obj;
        return this.getMillisFromUnix() == other.getMillisFromUnix();
    }

    @Override
    public int hashCode() {
        return getValue().hashCode();
    }

    @Override
    public <Result, Arg> Result accept(ScalarValueVisitor<Result, Arg> visitor, Arg arg) {
        return visitor.visit(this, arg);
    }

}
