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
import com.torodb.torod.core.subdocument.values.LongValue;
import org.jooq.DataType;
import org.jooq.impl.SQLDataType;

/**
 *
 */
public class LongValueConverter implements SubdocValueConverter<Long, LongValue>{
    private static final long serialVersionUID = 1L;

    @Override
    public DataType<Long> getDataType() {
        return SQLDataType.BIGINT;
    }

    @Override
    public BasicType getErasuredType() {
        return BasicType.LONG;
    }

    @Override
    public LongValue from(Long databaseObject) {
        return new LongValue(databaseObject);
    }

    @Override
    public Long to(LongValue userObject) {
        return userObject.getValue();
    }

    @Override
    public Class<Long> fromType() {
        return Long.class;
    }

    @Override
    public Class<LongValue> toType() {
        return LongValue.class;
    }
    
}
