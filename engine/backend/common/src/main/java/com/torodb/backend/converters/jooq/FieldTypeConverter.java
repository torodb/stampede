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

package com.torodb.backend.converters.jooq;

import com.torodb.core.transaction.metainf.FieldType;
import org.jooq.Converter;
import org.jooq.DataType;
import org.jooq.impl.SQLDataType;

/**
 *
 */
public class FieldTypeConverter implements Converter<String, FieldType> {

  private static final long serialVersionUID = 1L;

  public static final DataType<FieldType> TYPE = SQLDataType.VARCHAR.asConvertedDataType(
      new FieldTypeConverter());

  @Override
  public FieldType from(String databaseObject) {
    return FieldType.valueOf(databaseObject);
  }

  @Override
  public String to(FieldType userObject) {
    return userObject.name();
  }

  @Override
  public Class<String> fromType() {
    return String.class;
  }

  @Override
  public Class<FieldType> toType() {
    return FieldType.class;
  }

}
