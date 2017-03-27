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
import com.torodb.backend.postgresql.converters.util.SqlEscaper;
import com.torodb.common.util.TextEscaper;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class JsonbSqlBinding extends StringSqlBinding {

  public static final JsonbSqlBinding INSTANCE = new JsonbSqlBinding();


  @Override
  public String getPlaceholder() {
    return "?::jsonb";
  }

}
