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
import com.torodb.backend.udt.MongoTimestampUDT;
import com.torodb.backend.udt.record.MongoTimestampRecord;
import org.postgresql.util.PGobject;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class MongoTimestampRecordSqlBinding implements SqlBinding<MongoTimestampRecord> {

  public static final MongoTimestampRecordSqlBinding INSTANCE =
      new MongoTimestampRecordSqlBinding();

  @Override
  public MongoTimestampRecord get(ResultSet resultSet, int columnIndex) throws SQLException {
    PGobject pgObject = (PGobject) resultSet.getObject(columnIndex);

    if (pgObject == null) {
      return null;
    }

    String value = pgObject.getValue();
    int indexOfComma = value.indexOf(',');
    Integer secs = Integer.parseInt(value.substring(1, indexOfComma));
    Integer count = Integer.parseInt(value.substring(indexOfComma + 1, value.length() - 1));
    return new MongoTimestampRecord(secs, count);
  }

  @Override
  public void set(PreparedStatement preparedStatement, int parameterIndex,
      MongoTimestampRecord value)
      throws SQLException {
    preparedStatement.setString(parameterIndex, "(" + value.getSecs() + ',' + value.getCounter()
        + ')');
  }

  @Override
  public String getPlaceholder() {
    return "?::\"" + TorodbSchema.IDENTIFIER + "\".\"" + MongoTimestampUDT.IDENTIFIER + '"';
  }
}
