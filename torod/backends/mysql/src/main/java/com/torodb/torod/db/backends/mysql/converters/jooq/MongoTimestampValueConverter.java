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
 *     Copyright (c) 2016, 8Kdata Technology
 *     
 */

package com.torodb.torod.db.backends.mysql.converters.jooq;

import java.math.BigInteger;

import org.jooq.impl.SQLDataType;
import org.jooq.types.ULong;

import com.torodb.torod.core.subdocument.ScalarType;
import com.torodb.torod.core.subdocument.values.ScalarMongoTimestamp;
import com.torodb.torod.core.subdocument.values.heap.DefaultScalarMongoTimestamp;
import com.torodb.torod.db.backends.converters.jooq.DataTypeForScalar;
import com.torodb.torod.db.backends.converters.jooq.SubdocValueConverter;

/**
 *
 */
public class MongoTimestampValueConverter implements
        SubdocValueConverter<ULong, ScalarMongoTimestamp> {

    private static final long serialVersionUID = 1251948867583783920L;

    public static final DataTypeForScalar<ScalarMongoTimestamp> TYPE = DataTypeForScalar.from(SQLDataType.BIGINTUNSIGNED, new MongoTimestampValueConverter());

    @Override
    public ScalarType getErasuredType() {
        return ScalarType.MONGO_TIMESTAMP;
    }

    @Override
    public ScalarMongoTimestamp from(ULong databaseObject) {
        BigInteger value = databaseObject.toBigInteger();
        return new DefaultScalarMongoTimestamp(value.shiftRight(32).intValue(), value.shiftLeft(32).shiftRight(32).intValue());
    }

    @Override
    public ULong to(ScalarMongoTimestamp userObject) {
        return ULong.valueOf(BigInteger.valueOf(userObject.getSecondsSinceEpoch()).shiftLeft(32).add(BigInteger.valueOf(userObject.getOrdinal())));
    }

    @Override
    public Class<ULong> fromType() {
        return ULong.class;
    }

    @Override
    public Class<ScalarMongoTimestamp> toType() {
        return ScalarMongoTimestamp.class;
    }
}
