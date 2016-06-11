/*
 * This file is part of ToroDB.
 *
 * ToroDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * ToroDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with common. If not, see <http://www.gnu.org/licenses/>.
 *
 * Copyright (C) 2016 8Kdata.
 * 
 */

package com.torodb.torod.db.backends.mysql.converters.json;

import java.math.BigInteger;

import org.jooq.types.ULong;

import com.torodb.torod.core.subdocument.values.ScalarMongoTimestamp;
import com.torodb.torod.core.subdocument.values.heap.DefaultScalarMongoTimestamp;
import com.torodb.torod.db.backends.converters.ValueConverter;

/**
 *
 */
public class MongoTimestampValueToJsonConverter implements
        ValueConverter<Long, ScalarMongoTimestamp> {

    private static final long serialVersionUID = 1L;

    @Override
    public Class<? extends Long> getJsonClass() {
        return Long.class;
    }

    @Override
    public Class<? extends ScalarMongoTimestamp> getValueClass() {
        return ScalarMongoTimestamp.class;
    }

    @Override
    public ScalarMongoTimestamp toValue(Long value) {
        BigInteger bigValue = ULong.valueOf(value).toBigInteger();
        return new DefaultScalarMongoTimestamp(bigValue.shiftRight(32).intValue(), bigValue.shiftLeft(32).shiftRight(32).intValue());
    }

}
