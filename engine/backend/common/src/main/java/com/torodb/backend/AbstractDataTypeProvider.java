/*
 * ToroDB
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
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.torodb.backend;

import com.google.common.collect.ImmutableMap;
import com.torodb.backend.converters.jooq.DataTypeForKv;
import com.torodb.core.transaction.metainf.FieldType;

import javax.inject.Singleton;

/**
 *
 */
@Singleton
public abstract class AbstractDataTypeProvider implements DataTypeProvider {

  private final ImmutableMap<FieldType, DataTypeForKv<?>> dataTypes;

  protected AbstractDataTypeProvider(ImmutableMap<FieldType, DataTypeForKv<?>> dataTypes) {
    this.dataTypes = ImmutableMap.<FieldType, DataTypeForKv<?>>builder()
        .putAll(dataTypes)
        .build();

    //Check that all data types are specified or throw IllegalArgumentException
    for (FieldType fieldType : FieldType.values()) {
      getDataType(fieldType);
    }
  }

  @Override
  public DataTypeForKv<?> getDataType(FieldType type) {
    DataTypeForKv<?> dataType = dataTypes.get(type);
    if (dataType == null) {
      throw new IllegalArgumentException("It is not defined how to map elements of type " + type
          + " to SQL");
    }
    return dataType;
  }
}
