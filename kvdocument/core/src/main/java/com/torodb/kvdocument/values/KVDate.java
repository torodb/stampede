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
package com.torodb.kvdocument.values;

import com.torodb.kvdocument.types.DateType;
import org.threeten.bp.LocalDate;

/**
 *
 */
public abstract class KVDate extends KVValue<LocalDate> {

    private static final long serialVersionUID = -218788179965687517L;

    @Override
    public DateType getType() {
        return DateType.INSTANCE;
    }

    @Override
    public Class<? extends LocalDate> getValueClass() {
        return LocalDate.class;
    }

    @Override
    public String toString() {
        return getValue().toString();
    }

    @Override
    public int hashCode() {
        return getValue().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof KVDate)) {
            return false;
        }
        KVDate other = (KVDate) obj;
        return this.getValue().equals(other.getValue());
    }

    @Override
    public <Result, Arg> Result accept(KVValueVisitor<Result, Arg> visitor, Arg arg) {
        return visitor.visit(this, arg);
    }
}
