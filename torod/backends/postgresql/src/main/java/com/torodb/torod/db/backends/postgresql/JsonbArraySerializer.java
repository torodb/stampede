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


package com.torodb.torod.db.backends.postgresql;

import com.google.common.base.Preconditions;
import com.torodb.torod.db.backends.ArraySerializer;
import org.jooq.Condition;
import org.jooq.Table;
import org.jooq.impl.DSL;

import javax.annotation.Nonnull;
import javax.inject.Singleton;

/**
 *
 */
@Singleton
public class JsonbArraySerializer implements ArraySerializer {

    private static final long serialVersionUID = 946395628;

    @Override
    public String getFieldName(String[] keys) {
        return getFieldName(keys, 0, keys.length);
    }

    @Override
    public String getFieldName(String[] keys, int from, int to) {
        Preconditions.checkArgument(to > from, "'to' must be greater than 'from'");

        StringBuilder sb = new StringBuilder(12 + (to - from + 1) * 2);

        for (int i = from; i < to; i++) {
            sb.append('"').append(keys[i]).append('"')
                    .append("->");
        }
        sb.delete(sb.length() - 2, sb.length()); //last '->' must be removed

        return sb.toString();
    }

    @Nonnull
    @Override
    public Condition typeof(String fieldName, String typeName) {
        return DSL.condition("jsonb_typeof(?) = '?'", fieldName, typeName);
    }

    @Nonnull
    @Override
    public Condition typeof(String fieldName, String typeName, Condition condition) {
        return typeof(fieldName, typeName).and(condition);
    }

    @Nonnull
    @Override
    public Condition arrayLength(String fieldName, String value) {
        return DSL.condition("jsonb_array_length(?) = '?'", fieldName, value);
    }

    @Nonnull
    @Override
    public Table arrayElements(String fieldName) {
        return DSL.table("jsonb_array_elements(?)", fieldName);
    }

    @Override
    public String translateValue(Object translatedObject) {
        return (translatedObject instanceof String) ?
                '\"' + (String) translatedObject + '\"'
                : translatedObject.toString();
    }
}
