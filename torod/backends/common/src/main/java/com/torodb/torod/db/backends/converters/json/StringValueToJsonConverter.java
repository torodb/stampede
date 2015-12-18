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

package com.torodb.torod.db.backends.converters.json;

import com.torodb.torod.db.backends.converters.ValueConverter;
import com.torodb.torod.core.subdocument.values.StringValue;

/**
 *
 */
public class StringValueToJsonConverter implements
        ValueConverter<String, StringValue> {

    @Override
    public Class<? extends String> getJsonClass() {
        return String.class;
    }

    @Override
    public Class<? extends StringValue> getValueClass() {
        return StringValue.class;
    }

    @Override
    public String toJson(StringValue value) {
        return value.getValue();
    }

    @Override
    public StringValue toValue(String value) {
        return new StringValue(value);
    }
    
}
