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

import javax.json.JsonString;

import com.google.common.io.ByteSource;
import com.torodb.common.util.HexUtils;
import com.torodb.kvdocument.values.KVBinary.KVBinarySubtype;
import com.torodb.torod.core.subdocument.values.ScalarBinary;
import com.torodb.torod.core.subdocument.values.heap.ByteSourceScalarBinary;

/**
 *
 */
public class BinaryToArrayConverter implements ArrayConverter<JsonString, ScalarBinary> {
    private static final long serialVersionUID = 1L;

    @Override
    public String toJsonLiteral(ScalarBinary value) {
        return value.toString();
    }

    @Override
    public ScalarBinary fromJsonValue(JsonString value) {
        byte[] bytes = HexUtils.hex2Bytes(value.getString());
        return new ByteSourceScalarBinary(
                KVBinarySubtype.MONGO_GENERIC,
                (byte) 0,
                ByteSource.wrap(bytes)
        );
    }
}
