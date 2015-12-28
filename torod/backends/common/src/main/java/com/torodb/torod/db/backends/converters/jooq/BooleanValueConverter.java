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

package com.torodb.torod.db.backends.converters.jooq;

import com.torodb.torod.core.subdocument.BasicType;
import com.torodb.torod.core.subdocument.values.BooleanValue;
import org.jooq.DataType;
import org.jooq.impl.SQLDataType;

/**
 *
 */
public class BooleanValueConverter implements SubdocValueConverter<Boolean, BooleanValue> {
    private static final long serialVersionUID = 1L;

    @Override
    public DataType<Boolean> getDataType() {
        return SQLDataType.BOOLEAN;
    }

    @Override
    public BasicType getErasuredType() {
        return BasicType.BOOLEAN;
    }

    @Override
    public BooleanValue from(Boolean databaseObject) {
        return BooleanValue.from(databaseObject);
    }

    @Override
    public Boolean to(BooleanValue userObject) {
        return userObject.getValue();
    }

    @Override
    public Class<Boolean> fromType() {
        return Boolean.class;
    }

    @Override
    public Class<BooleanValue> toType() {
        return BooleanValue.class;
    }

}
