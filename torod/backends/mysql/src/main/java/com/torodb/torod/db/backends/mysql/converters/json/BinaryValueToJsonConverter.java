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

package com.torodb.torod.db.backends.mysql.converters.json;

import javax.xml.bind.DatatypeConverter;

import com.google.common.io.ByteSource;
import com.torodb.kvdocument.values.KVBinary.KVBinarySubtype;
import com.torodb.torod.core.exceptions.ToroImplementationException;
import com.torodb.torod.core.subdocument.values.ScalarBinary;
import com.torodb.torod.core.subdocument.values.heap.ByteSourceScalarBinary;
import com.torodb.torod.db.backends.converters.ValueConverter;

/**
 *
 */
public class BinaryValueToJsonConverter implements
        ValueConverter<String, ScalarBinary> {

    private static final long serialVersionUID = 1L;

    @Override
    public Class<? extends String> getJsonClass() {
        return String.class;
    }

    @Override
    public Class<? extends ScalarBinary> getValueClass() {
        return ScalarBinary.class;
    }

    @Override
    public ScalarBinary toValue(String value) {
        if (!value.startsWith("base64:type15:")) {
            throw new ToroImplementationException(
                    "A binary in escape format was expected, but " + value
                    + " was found"
            );
        }
        return new ByteSourceScalarBinary(KVBinarySubtype.MONGO_GENERIC,
                (byte) 0,
                ByteSource.wrap(DatatypeConverter.parseBase64Binary(value.substring(14))));
    }
}
