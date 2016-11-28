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

package com.torodb.backend.converters.sql;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class BinarySqlBinding implements SqlBinding<byte[]> {

  public static final BinarySqlBinding INSTANCE = new BinarySqlBinding();

  @Override
  @SuppressFBWarnings(value = "PZLA_PREFER_ZERO_LENGTH_ARRAYS",
      justification = "Null value has different meaning from empty array value")
  public byte[] get(ResultSet resultSet, int columnIndex) throws SQLException {
    byte[] value = resultSet.getBytes(columnIndex);
    if (resultSet.wasNull()) {
      return null;
    }
    return value;
  }

  @Override
  public void set(PreparedStatement preparedStatement, int parameterIndex, byte[] value) throws
      SQLException {
    preparedStatement.setBytes(parameterIndex, value);
  }
}
