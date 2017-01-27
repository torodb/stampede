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

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class Decimal128SqlBinding implements SqlBinding<BigDecimal> {

  public static final Decimal128SqlBinding INSTANCE = new Decimal128SqlBinding();

  @Override
  public BigDecimal get(ResultSet resultSet, int columnIndex) throws SQLException {
    BigDecimal value = resultSet.getBigDecimal(columnIndex);
    if (resultSet.wasNull()) {
      return null;
    }
    return value;
  }

  @Override
  public void set(PreparedStatement preparedStatement, int parameterIndex, BigDecimal value)
      throws SQLException {
    preparedStatement.setBigDecimal(parameterIndex, value);
  }
}
