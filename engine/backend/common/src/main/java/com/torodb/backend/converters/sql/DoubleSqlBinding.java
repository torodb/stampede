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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DoubleSqlBinding implements SqlBinding<Double> {

  public static final DoubleSqlBinding INSTANCE = new DoubleSqlBinding();

  @Override
  public Double get(ResultSet resultSet, int columnIndex) throws SQLException {
    double value = resultSet.getDouble(columnIndex);
    if (resultSet.wasNull()) {
      return null;
    }
    return value;
  }

  @Override
  public void set(PreparedStatement preparedStatement, int parameterIndex, Double value) throws
      SQLException {
    preparedStatement.setDouble(parameterIndex, value);
  }
}
