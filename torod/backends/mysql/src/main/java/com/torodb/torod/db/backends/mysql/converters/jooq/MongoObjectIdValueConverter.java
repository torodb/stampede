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

package com.torodb.torod.db.backends.mysql.converters.jooq;

import org.jooq.impl.SQLDataType;

import com.torodb.torod.core.subdocument.ScalarType;
import com.torodb.torod.core.subdocument.values.ScalarMongoObjectId;
import com.torodb.torod.core.subdocument.values.heap.ByteArrayScalarMongoObjectId;
import com.torodb.torod.db.backends.converters.jooq.DataTypeForScalar;
import com.torodb.torod.db.backends.converters.jooq.SubdocValueConverter;

/**
 *
 */
public class MongoObjectIdValueConverter implements SubdocValueConverter<byte[], ScalarMongoObjectId> {
    private static final long serialVersionUID = 1L;

    public static final DataTypeForScalar<ScalarMongoObjectId> TYPE = DataTypeForScalar.from(SQLDataType.BINARY, new MongoObjectIdValueConverter());

    @Override
    public ScalarType getErasuredType() {
        return ScalarType.MONGO_OBJECT_ID;
    }

    @Override
    public ScalarMongoObjectId from(byte[] databaseObject) {
        return new ByteArrayScalarMongoObjectId(databaseObject);
    }

    @Override
    public byte[] to(ScalarMongoObjectId userObject) {
        return userObject.getArrayValue();
    }

    @Override
    public Class<byte[]> fromType() {
        return byte[].class;
    }

    @Override
    public Class<ScalarMongoObjectId> toType() {
        return ScalarMongoObjectId.class;
    }

}
