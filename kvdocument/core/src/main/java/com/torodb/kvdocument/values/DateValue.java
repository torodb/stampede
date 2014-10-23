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
import com.torodb.kvdocument.types.DocType;
import java.util.Objects;
import org.threeten.bp.LocalDate;

/**
 *
 */
public class DateValue implements DocValue {

    private final LocalDate value;

    public DateValue(LocalDate value) {
        this.value = value;
    }

    @Override
    public LocalDate getValue() {
        return value;
    }

    @Override
    public String toString() {
        return value.toString();
    }

    @Override
    public DocType getType() {
        return DateType.INSTANCE;
    }
    
    @Override
    public <Result, Arg> Result accept(DocValueVisitor<Result, Arg> visitor, Arg arg) {
        return visitor.visit(this, arg);
    }
}
