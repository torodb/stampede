/*
 * ToroDB - ToroDB: Backend common
 * Copyright © 2014 8Kdata Technology (www.8kdata.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.torodb.backend.converters.json;

import java.time.LocalDate;

import com.torodb.backend.converters.ValueConverter;
import com.torodb.kvdocument.values.KVDate;
import com.torodb.kvdocument.values.heap.LocalDateKVDate;

/**
 *
 */
public class DateValueToJsonConverter implements ValueConverter<String, KVDate> {

    private static final long serialVersionUID = 1L;

    @Override
    public Class<? extends String> getJsonClass() {
        return String.class;
    }

    @Override
    public Class<? extends KVDate> getValueClass() {
        return KVDate.class;
    }

    @Override
    public KVDate toValue(String value) {
        return new LocalDateKVDate(LocalDate.parse(value));
    }
    
}
