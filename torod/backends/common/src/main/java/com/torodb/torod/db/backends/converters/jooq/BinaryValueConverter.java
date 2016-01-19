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
import com.torodb.torod.core.subdocument.values.BinaryValue;
import org.jooq.DataType;
import org.jooq.impl.SQLDataType;

/**
 *
 */
public class BinaryValueConverter implements
        SubdocValueConverter<byte[], BinaryValue> {
    private static final long serialVersionUID = 1L;

    @Override
    public DataType<byte[]> getDataType() {
        return SQLDataType.BINARY;
    }

    @Override
    public BasicType getErasuredType() {
        return BasicType.BINARY;
    }

    @Override
    public BinaryValue from(byte[] databaseObject) {
        return new BinaryValue(databaseObject);
    }

    @Override
    public byte[] to(BinaryValue userObject) {
        return userObject.getArrayValue();
    }

    @Override
    public Class<byte[]> fromType() {
        return byte[].class;
    }

    @Override
    public Class<BinaryValue> toType() {
        return BinaryValue.class;
    }

}
