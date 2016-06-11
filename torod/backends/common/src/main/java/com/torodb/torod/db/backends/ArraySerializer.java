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

package com.torodb.torod.db.backends;

import org.jooq.Condition;
import org.jooq.Field;
import org.jooq.Param;
import org.jooq.Table;

import javax.annotation.Nonnull;
import java.io.Serializable;

/**
 * Interface to abstract database-specific way of array serialization
 */
public interface ArraySerializer extends Serializable {

    String getFieldName(String[] keys);

    String getFieldName(String[] keys, int from, int to);

    @Nonnull Condition typeof(String fieldName, String typeName);

    @Nonnull Condition typeof(String fieldName, String typeName, Condition condition);

    @Nonnull Condition arrayLength(String fieldName, Param<?> value);

    @Nonnull Table arrayElements(Field<?> iteratorVariable, Field<?> field);

}
