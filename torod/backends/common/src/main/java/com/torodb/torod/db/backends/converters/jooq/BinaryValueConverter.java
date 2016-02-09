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

import com.google.common.io.ByteSource;
import com.torodb.kvdocument.values.KVBinary.KVBinarySubtype;
import com.torodb.torod.core.subdocument.ScalarType;
import com.torodb.torod.core.subdocument.values.ScalarBinary;
import com.torodb.torod.core.subdocument.values.heap.ByteSourceScalarBinary;
import org.jooq.DataType;
import org.jooq.impl.SQLDataType;

/**
 *
 */
public class BinaryValueConverter implements
        SubdocValueConverter<byte[], ScalarBinary> {
    private static final long serialVersionUID = 1L;

    @Override
    public DataType<byte[]> getDataType() {
        return SQLDataType.BINARY;
    }

    @Override
    public ScalarType getErasuredType() {
        return ScalarType.BINARY;
    }

    @Override
    public ScalarBinary from(byte[] databaseObject) {
        return new ByteSourceScalarBinary(KVBinarySubtype.MONGO_GENERIC, (byte) 0, ByteSource.wrap(databaseObject));
    }

    @Override
    public byte[] to(ScalarBinary userObject) {
        return userObject.getByteSource().read();
    }

    @Override
    public Class<byte[]> fromType() {
        return byte[].class;
    }

    @Override
    public Class<ScalarBinary> toType() {
        return ScalarBinary.class;
    }

}
