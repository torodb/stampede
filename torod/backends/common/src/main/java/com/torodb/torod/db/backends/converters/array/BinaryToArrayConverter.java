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

package com.torodb.torod.db.backends.converters.array;

import com.google.common.io.ByteSource;
import com.torodb.common.util.HexUtils;
import com.torodb.kvdocument.values.KVBinary.KVBinarySubtype;
import com.torodb.torod.core.subdocument.values.ScalarBinary;
import com.torodb.torod.core.subdocument.values.heap.ByteSourceScalarBinary;
import com.torodb.torod.db.backends.converters.ValueConverter;

/**
 *
 */
public class BinaryToArrayConverter implements
        ValueConverter<String, ScalarBinary> {

    @Override
    public Class<? extends String> getJsonClass() {
        return String.class;
    }

    @Override
    public Class<? extends ScalarBinary> getValueClass() {
        return ScalarBinary.class;
    }

    @Override
    public String toJson(ScalarBinary value) {
        return value.toString();
    }

    @Override
    public ScalarBinary toValue(String value) {
        byte[] bytes = HexUtils.hex2Bytes(value);
        return new ByteSourceScalarBinary(
                KVBinarySubtype.MONGO_GENERIC,
                (byte) 0,
                ByteSource.wrap(bytes)
        );
    }
    
}
