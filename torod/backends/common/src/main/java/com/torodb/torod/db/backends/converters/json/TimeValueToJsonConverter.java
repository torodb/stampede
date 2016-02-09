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

import com.torodb.torod.core.subdocument.values.ScalarTime;
import com.torodb.torod.core.subdocument.values.heap.LocalTimeScalarTime;
import com.torodb.torod.db.backends.converters.ValueConverter;
import org.threeten.bp.LocalTime;

/**
 *
 */
public class TimeValueToJsonConverter implements
        ValueConverter<String, ScalarTime> {

    @Override
    public Class<? extends String> getJsonClass() {
        return String.class;
    }

    @Override
    public Class<? extends ScalarTime> getValueClass() {
        return ScalarTime.class;
    }

    @Override
    public String toJson(ScalarTime value) {
        return value.getValue().toString();
    }

    @Override
    public ScalarTime toValue(String value) {
        return new LocalTimeScalarTime(LocalTime.parse(value));
    }
    
}
