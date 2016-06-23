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

package com.torodb.backend.postgresql.converters.jooq;

import org.jooq.util.postgres.PostgresDataType;

import com.google.common.io.ByteSource;
import com.torodb.backend.converters.jooq.DataTypeForKV;
import com.torodb.backend.converters.jooq.KVValueConverter;
import com.torodb.backend.converters.sql.BinarySqlBinding;
import com.torodb.backend.converters.sql.SqlBinding;
import com.torodb.kvdocument.types.BinaryType;
import com.torodb.kvdocument.types.KVType;
import com.torodb.kvdocument.values.KVBinary;
import com.torodb.kvdocument.values.KVBinary.KVBinarySubtype;
import com.torodb.kvdocument.values.heap.ByteSourceKVBinary;

/**
 *
 */
public class BinaryValueConverter implements
        KVValueConverter<byte[], KVBinary> {
    private static final long serialVersionUID = 1L;

    public static final DataTypeForKV<KVBinary> TYPE = DataTypeForKV.from(PostgresDataType.BYTEA, new BinaryValueConverter());

    @Override
    public KVType getErasuredType() {
        return BinaryType.INSTANCE;
    }

    @Override
    public KVBinary from(byte[] databaseObject) {
        return new ByteSourceKVBinary(KVBinarySubtype.MONGO_GENERIC, (byte) 0, ByteSource.wrap(databaseObject));
    }

    @Override
    public byte[] to(KVBinary userObject) {
        return userObject.getByteSource().read();
    }

    @Override
    public Class<byte[]> fromType() {
        return byte[].class;
    }

    @Override
    public Class<KVBinary> toType() {
        return KVBinary.class;
    }

    @Override
    public SqlBinding<byte[]> getSqlBinding() {
        return BinarySqlBinding.INSTANCE;
    }

}
