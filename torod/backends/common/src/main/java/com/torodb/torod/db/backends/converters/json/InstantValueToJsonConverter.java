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

import org.threeten.bp.Instant;
import org.threeten.bp.ZoneOffset;
import org.threeten.bp.format.DateTimeFormatter;
import org.threeten.bp.format.DateTimeFormatterBuilder;
import org.threeten.bp.format.ResolverStyle;

import static org.threeten.bp.temporal.ChronoField.DAY_OF_MONTH;
import static org.threeten.bp.temporal.ChronoField.MONTH_OF_YEAR;
import static org.threeten.bp.temporal.ChronoField.YEAR;
import static org.threeten.bp.temporal.ChronoField.HOUR_OF_DAY;
import static org.threeten.bp.temporal.ChronoField.MINUTE_OF_HOUR;
import static org.threeten.bp.temporal.ChronoField.SECOND_OF_MINUTE;
import static org.threeten.bp.temporal.ChronoField.MILLI_OF_SECOND;

import com.torodb.torod.core.subdocument.values.ScalarInstant;
import com.torodb.torod.core.subdocument.values.heap.InstantScalarInstant;
import com.torodb.torod.db.backends.converters.ValueConverter;

/**
 *
 */
public class InstantValueToJsonConverter implements
        ValueConverter<String, ScalarInstant> {

    public static final DateTimeFormatter ISO_INSTANT_UTC;
    static {
        ISO_INSTANT_UTC = new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .appendValue(YEAR, 4)
            .appendLiteral('-')
            .appendValue(MONTH_OF_YEAR, 2)
            .appendLiteral('-')
            .appendValue(DAY_OF_MONTH, 2)
            .appendLiteral('T')
            .appendValue(HOUR_OF_DAY, 2)
            .appendLiteral(':')
            .appendValue(MINUTE_OF_HOUR, 2)
            .appendLiteral(':')
            .appendValue(SECOND_OF_MINUTE, 2)
            .appendLiteral('.')
            .appendValue(MILLI_OF_SECOND, 3)
            .toFormatter()
            .withResolverStyle(ResolverStyle.STRICT)
            .withZone(ZoneOffset.UTC);
    }

    @Override
    public Class<? extends String> getJsonClass() {
        return String.class;
    }

    @Override
    public Class<? extends ScalarInstant> getValueClass() {
        return ScalarInstant.class;
    }

    @Override
    public String toJson(ScalarInstant value) {
        return value.getValue().toString();
    }

    @Override
    public ScalarInstant toValue(String value) {
        return new InstantScalarInstant(Instant.from(ISO_INSTANT_UTC.parse(value)));
    }
    
    
}
