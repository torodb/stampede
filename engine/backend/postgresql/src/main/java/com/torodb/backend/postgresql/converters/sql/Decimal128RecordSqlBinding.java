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

package com.torodb.backend.postgresql.converters.sql;

import com.torodb.backend.converters.sql.SqlBinding;
import com.torodb.backend.meta.TorodbSchema;
import com.torodb.backend.udt.Decimal128UDT;
import com.torodb.backend.udt.MongoTimestampUDT;
import com.torodb.backend.udt.record.Decimal128Record;
import com.torodb.backend.udt.record.MongoTimestampRecord;
import org.postgresql.util.PGobject;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class Decimal128RecordSqlBinding implements SqlBinding<Decimal128Record> {

  public static final Decimal128RecordSqlBinding INSTANCE =
      new Decimal128RecordSqlBinding();

  @Override
  public Decimal128Record get(ResultSet resultSet, int columnIndex) throws SQLException {
    PGobject pgObject = (PGobject) resultSet.getObject(columnIndex);

    if (pgObject == null) {
      return null;
    }

    //We parse it in this ugly way instead of splitting the String with the comma
    //because we don't know how the DB is going to store the numeric field.
    //Normally it will be a 123.41234 format but some DDBB may use a comma instead
    //so we decide to go backwards

    String value = pgObject.getValue();
    value = value.substring(1, value.length() - 1);
    int indexOfComma = value.lastIndexOf(',');

    boolean isNegZero = value.substring(indexOfComma + 1, value.length()).charAt(0)=='t';
    value = value.substring(0, indexOfComma);
    indexOfComma = value.lastIndexOf(',');

    boolean isNan = value.substring(indexOfComma + 1, value.length()).charAt(0)=='t';
    value = value.substring(0, indexOfComma);
    indexOfComma = value.lastIndexOf(',');

    boolean isInfinity = value.substring(indexOfComma + 1, value.length()).charAt(0)=='t';
    value = value.substring(0, indexOfComma);

    BigDecimal bigDec = new BigDecimal(value);

    return new Decimal128Record(bigDec, isInfinity, isNan, isNegZero);
  }

  @Override
  public void set(PreparedStatement preparedStatement, int parameterIndex,
                  Decimal128Record value)
      throws SQLException {
    preparedStatement.setString(parameterIndex, "("
            + value.getValue() + ','
            + value.getInfinity() + ','
            + value.getNan() + ','
            + value.getNegativeZero()
        + ')');
  }

  @Override
  public String getPlaceholder() {
    return "?::\"" + TorodbSchema.IDENTIFIER + "\".\"" + Decimal128UDT.IDENTIFIER + '"';
  }
}
