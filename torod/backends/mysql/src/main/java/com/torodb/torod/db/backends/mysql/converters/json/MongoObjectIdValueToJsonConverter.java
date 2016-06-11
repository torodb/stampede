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

import com.torodb.torod.core.exceptions.ToroImplementationException;
import com.torodb.torod.core.subdocument.values.ScalarMongoObjectId;
import com.torodb.torod.core.subdocument.values.heap.ByteArrayScalarMongoObjectId;
import com.torodb.torod.db.backends.converters.ValueConverter;

/**
 *
 */
public class MongoObjectIdValueToJsonConverter implements
        ValueConverter<String, ScalarMongoObjectId> {

    private static final long serialVersionUID = 1L;

    @Override
    public Class<? extends String> getJsonClass() {
        return String.class;
    }

    @Override
    public Class<? extends ScalarMongoObjectId> getValueClass() {
        return ScalarMongoObjectId.class;
    }

    @Override
    public ScalarMongoObjectId toValue(String value) {
        if (!value.startsWith("base64:type254:")) {
            throw new ToroImplementationException(
                    "A binary in escape format was expected, but " + value
                    + " was found"
            );
        }
        return new ByteArrayScalarMongoObjectId(DatatypeConverter.parseBase64Binary(value.substring(15)));
    }
}
